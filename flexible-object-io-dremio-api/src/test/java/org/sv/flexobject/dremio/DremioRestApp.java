package org.sv.flexobject.dremio;


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.aws.AWSSecretProviderWithVault;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;
import org.sv.flexobject.connections.SecretProvider;
import org.sv.flexobject.docker.DockerClientProvider;
import org.sv.flexobject.docker.DockerUtils;
import org.sv.flexobject.dremio.api.CatalogAPI;
import org.sv.flexobject.dremio.api.DremioApiException;
import org.sv.flexobject.dremio.api.UserAPI;
import org.sv.flexobject.dremio.domain.catalog.*;
import org.sv.flexobject.dremio.domain.catalog.config.AmazonS3Conf;
import org.sv.flexobject.dremio.domain.catalog.config.CredentialType;
import org.sv.flexobject.dremio.domain.user.User;
import org.sv.flexobject.properties.FilePropertiesProvider;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;

public class DremioRestApp {

    public static final Logger logger = LogManager.getLogger(DremioRestApp.class);
    public static final String DREMIO_DOCKER_IMAGE_NAME = "dremio/dremio-oss";
    public static final List<Integer> DREMIO_PORTS = asList(9047, 31010, 45678);

    public static final String INTEGRATION_TEST_USERNAME = "testUser";
    // password must be at least 8 letters long,
    // must contain at least one number and one letter
    public static final String INTEGRATION_TEST_PASSWORD = "testPassword123#";

    public static final String DREMIO_CONTAINER_NAME = "TestDremio";

    public static Container dremioContainer;
    static DremioClient client;
    static DremioClient enterpriseClient;
    static Boolean enterpriseClientNotAvailable;

    static Space testSpace;
    static View zips;

    static PropertiesProvider propertiesProvider = new FilePropertiesProvider("src/test/resources/db");
    static {
        SecretProvider secretProvider = new SecretProvider() {
            @Override
            public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
                return null;
            }

            @Override
            public Object getSecret(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
                return "dremioREST".equals(connectionName) ? INTEGRATION_TEST_PASSWORD : null;
            }
        };

        ConnectionManager.getInstance()
                .registerProvider(DremioClientProvider.class)
                .registerPropertiesProvider(propertiesProvider)
                .registerSecretProvider(secretProvider)
                .registerSecretProvider(new AWSSecretProviderWithVault())
                .setDeploymentLevel(ConnectionManager.DeploymentLevel.alpha);

    }

    static public DremioClientConf getDremioConf() throws Exception {
        Properties props = propertiesProvider.getProperties("dremioREST", ConnectionManager.DeploymentLevel.alpha, ConnectionManager.getInstance().getEnvironment());
        DremioClientConf conf = new DremioClientConf().from(props);
        return conf;
    }

    static public DremioClient getClient(){
        if (client == null) {
            try {
                startDremioInDocker();
                if (client == null) {
                    firstUser();
                    client = (DremioClient) ConnectionManager.getConnection(DremioClient.class, "dremioREST");
                }
                checkCreateSamples(client.catalog());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return client;
    }
    static public DremioClient getEnterpriseClient(){
        if (enterpriseClient == null && enterpriseClientNotAvailable == null) {
            try {
                enterpriseClient = (DremioClient) ConnectionManager.getConnection(DremioClient.class, "dremioREST_Enterprise");
                checkCreateSamples(enterpriseClient.catalog());
                enterpriseClientNotAvailable = false;
            } catch (Exception e) {
                enterpriseClientNotAvailable = true;
                return null;
            }
        }
        return enterpriseClient;
    }

    private static void checkCreateSamples(CatalogAPI catalogAPI) throws FileNotFoundException {
        Catalog catalog = catalogAPI.catalog();
        Optional<CatalogItem> item = catalog.findByPath("Samples");
        if (!item.isPresent()) {
            AmazonS3Conf conf = new AmazonS3Conf();
            conf.credentialType = CredentialType.NONE;
            conf.externalBucketList = Arrays.asList("samples.dremio.com");
            conf.rootPath = "/";
            catalogAPI.createSource("Samples", conf);
        }
        item = catalog.findByPath("TestSpace");
        if (!item.isPresent()) {
            testSpace = catalogAPI.createSpace("TestSpace");
        } else {
            testSpace = catalogAPI.getById(item.get().id);
        }
        item = catalog.findByPath("TestSpace.zips");
        if (!item.isPresent()){
            Table table = Table.builder(catalogAPI)
                    .startAt("Samples")
                    .subFolder("samples.dremio.com")
                    .dataset("zips.json")
                    .format(null, FormatType.JSON_FORMAT, true);
            zips = View.builder(catalogAPI)
                    .path("TestSpace", "zips")
                    .sql("select * from \"zips.json\"")
                    .context(table)
                    .create(null, false);
        } else {
            zips = catalogAPI.getById(item.get().id);
        }
    }

    private static void startDremioInDocker() {
        logger.info("Starting Dremio in Docker...");
        try {
            DockerClient dockerClient = DockerClientProvider.getDefault();
            logger.info("Got Docker client. Checking available images ...");

            Container dremioContainer = DockerUtils.getContainer(dockerClient, DREMIO_CONTAINER_NAME);
            if (dremioContainer != null) {
                if (DockerUtils.ContainerState.running == DockerUtils.ContainerState.valueOf(dremioContainer.getState())) {
                    try {
                        client = (DremioClient) ConnectionManager.getConnection(DremioClient.class, "dremioREST");
                        if (client != null) {
                            // Container is running and has correct admin credentials
                            return;
                        }
                    } catch (Exception e) {
                    }
                }
                // must delete container as otherwise it will retain that first user created
                dockerClient.removeContainerCmd(dremioContainer.getId()).withForce(true).exec();
            }


            Image image = DockerUtils.checkAndPullImage(dockerClient, DREMIO_DOCKER_IMAGE_NAME, "latest");
            assertNotNull(image);

            HostConfig hostConfig = HostConfig.newHostConfig()
                    .withPortBindings(DockerUtils.localhostPortBindings(DREMIO_PORTS));
            dockerClient.createContainerCmd(image.getId())
                    .withName(DREMIO_CONTAINER_NAME)
                    .withExposedPorts(DockerUtils.exposedTcpPorts(DREMIO_PORTS))
                    .withHostConfig(hostConfig)
                    .exec();

            dremioContainer = DockerUtils.getImageContainers(dockerClient, image).get(0);

            assertNotNull(dremioContainer);
            DockerUtils.startRestartContainer(dockerClient, dremioContainer);
            Thread.sleep(5000);
            System.out.println(DockerUtils.waitContainerRunning(dockerClient, dremioContainer));
        }catch (NotFoundException notFoundException) {
            logger.error("Docker unavailable");
        } catch (Exception e) {
            logger.error("Failed to start Dremio in Docker", e);
            throw new RuntimeException(e);
        }
    }

    static public void firstUser() throws Exception {
        User firstUser = User.builder()
                .name(INTEGRATION_TEST_USERNAME)
                .firstName("Test")
                .lastName("TestLastName")
                .email("foobar@foo.bar")
                .build();
        for (int i  = 0 ; i < 20 ; i++) {
            try {
                UserAPI.firstUser(DremioRestApp.getDremioConf(), firstUser, INTEGRATION_TEST_PASSWORD);
                System.out.println("Created first user");
                break;
            }catch (DremioApiException e){
                System.out.println("Exception : " + e);
                if (e.getMessage().endsWith("First user can only be created when no user is already registered")){
                    System.out.println("First user already exists");
                    return;
                }
                Thread.sleep(5000);
            }
        }
    }

    public static Space getTestSpace() {
        return testSpace;
    }

    public static View getZips() {
        return zips;
    }
}

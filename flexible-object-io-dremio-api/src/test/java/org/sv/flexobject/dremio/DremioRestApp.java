package org.sv.flexobject.dremio;


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.api.model.Container;
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
import software.amazon.awssdk.utils.StringUtils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.*;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;

public class DremioRestApp {

    public static final Logger logger = LogManager.getLogger(DremioRestApp.class);
    public static final String DREMIO_DOCKER_IMAGE_NAME = "dremio/dremio-oss";
    public static final List<Integer> DREMIO_PORTS = asList(9047, 31010, 32010, 45678);

    public static final String INTEGRATION_TEST_USERNAME = "testUser";
    // password must be at least 8 letters long,
    // must contain at least one number and one letter
    public static final String INTEGRATION_TEST_PASSWORD = "testPassword123#";

    public static final String DREMIO_CONTAINER_NAME = "TestDremio";

    static DremioClient client;
    static Boolean clientNotAvailable;
    static DremioClient enterpriseClient;

    static Boolean enterpriseClientNotAvailable;

    static Space testSpace;
    static View zips;


    static PropertiesProvider propertiesProvider = new FilePropertiesProvider("src/test/resources/db");

    private static DockerClient dockerClient = null;
    private static Container dremioContainer = null;
    private static String dremioIpAddress;
    private static Properties dremioProperties;

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
        if (dremioProperties == null) {
            dremioProperties = propertiesProvider.getProperties("dremioREST", ConnectionManager.DeploymentLevel.alpha, ConnectionManager.getInstance().getEnvironment());
        }
        DremioClientConf conf = new DremioClientConf().from(dremioProperties);
        if (dremioIpAddress != null)
            conf.hostname = dremioIpAddress;
        return conf;
    }

    static boolean checkPort(String hostname, int port, int iteration){
        try(Socket s = new Socket(hostname, port)){
            logger.info(String.format("--------------Port %s:%d is available", hostname, port));
            return true;
        } catch (IOException e) {
            logger.info(String.format("--------------Port %s:%d is not available", hostname, port));
            return false;
        }
    }
    static boolean checkPort(int port, int iteration){
        return checkPort("localhost", port, iteration);
    }
    static public DremioClient getClient(){
        if (client == null && clientNotAvailable == null) {
            try {
                startDremioInDocker();
                if (client == null) {
                    if (!firstUser()) {
                        clientNotAvailable = true;
                        throw new RuntimeException("Failed to connect to Dremio in Docker.");
                    }

                    client = (DremioClient) ConnectionManager.getConnection(DremioClient.class, "dremioREST");
                }
                checkCreateSamples(client.catalog());
                clientNotAvailable = false;
            } catch (Exception e) {
                clientNotAvailable = true;
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
            conf.externalBucketList = Collections.singletonList("samples.dremio.com");
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

    private static boolean connectClient() throws Exception {
        try {
            client = DremioClient.builder().forConf(getDremioConf()).withPassword(INTEGRATION_TEST_PASSWORD).build();
            if (client != null) {
                logger.debug("Dremio container already has first user");
                // Container is running and has correct admin credentials
                return true;
            }
        } catch (Exception e) {
            logger.debug("Dremio container does not have first user. ", e);
        }
        return false;
    }

    public static void dumpDremioStdin(Container dremioContainer) throws IOException {
        ResultCallback<Frame> callback = new ResultCallback.Adapter<Frame>() {
            @Override
            public void onNext(Frame item) {
                logger.debug("DREMIO:" + item.toString());
                super.onNext(item);
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("DREMIO:", throwable);
                super.onError(throwable);
            }
        };
        dockerClient
                .attachContainerCmd(dremioContainer.getId())
                .withLogs(true)
                .withStdOut(true)
                .withStdErr(true)
                .withFollowStream(true)
                .withTimestamps(true)
                .exec(callback);
    }

    private static boolean checkContainers(){
        try{
            List<Container> containers = dockerClient.listContainersCmd().withShowAll(true).exec();

            logger.debug("Containers:");
            for (Container container : containers) {
                logger.debug(container);
                if ("dremio/dremio-oss".equals(container.labels.get("org.opencontainers.image.title"))) {
                    if (!DockerUtils.isRunning(container)){
                        logger.info("Found Dead Dremio container with id: " + container.getId());
                        dockerClient.removeContainerCmd(container.getId()).withForce(true).exec();
                    } else {
                        dremioContainer = container;
                        logger.info("Found Running Dremio container with id: " + container.getId());
                        for (ContainerPort port : container.ports) {
                            logger.debug(port);
                        }
                        Map<String, ContainerNetwork> networks = container.getNetworkSettings().getNetworks();
                        if (networks.containsKey("bridge")) {
                            ContainerNetwork bridge = networks.get("bridge");
                            if (StringUtils.isNotBlank(bridge.getIpAddress())) {
                                dremioIpAddress = bridge.getIpAddress();
                                logger.info("Dremio IP address: " + dremioIpAddress);
                            }
                        }
                    }
                }
            }
        }catch (NotFoundException notFoundException) {
            logger.error("No Docker containers found", notFoundException);
            return false;
        }
        return true;
    }

    private static boolean waitForDremioPort(long timeoutMillis) throws Exception {
        long endTime = System.currentTimeMillis() + timeoutMillis;
        DremioClientConf conf = getDremioConf();
        do {
            if (checkPort(conf.getPort(), 0)) {
                dremioIpAddress = "localhost";
                logger.debug("Dremio is available on localhost");
                return true;
            } else if (checkPort(dremioIpAddress, conf.getPort(), 0)) {
                logger.debug("Dremio is available on " + dremioIpAddress);
                return true;
            }
            Thread.sleep(2000);
        } while (endTime > System.currentTimeMillis());

        return false;
    }

    private static boolean connectDremioInDocker() throws Exception {
        if (!checkContainers())
            return false;

        if (dremioContainer != null) {
            logger.debug("Got dremio container");
            if (DockerUtils.isRunning(dremioContainer)) {
                logger.debug("Dremio container is Running");

                if (waitForDremioPort(0)) {
                    if (connectClient())
                        return true;
                    logger.debug("Failed to connect to Dremio on the first try. Trying to create first user...");
                    if (firstUser(1)) {
                        logger.debug("Retrying connect to Dremio...");
                        if (connectClient()) {
                            logger.debug("Connected to Dremio");
                            return true;
                        }
                    }
                } else { // inspect what's wrong
                    dumpDremioStdin(dremioContainer);
                    logger.debug("Dremio is not available.");
                    dremioIpAddress = null;
                }
            } else {
                logger.debug("Dremio container is not running");
            }
        } else {
            logger.debug("No Dremio container found");
        }
        return false;
    }

    private static boolean connectDocker() throws Exception {
        if (dockerClient == null) {
            dockerClient = DockerClientProvider.getDefault();
            logger.info("Got Docker client.");
            try {
                Info info = dockerClient.infoCmd().exec();
                logger.info("Docker Info:" + info);
            } catch (NotFoundException notFoundException) {
                logger.error("Docker unavailable", notFoundException);
                return false;
            }
        }
        return true;
    }

    private static void startDremioInDocker() {
        logger.info("Starting Dremio in Docker...");
        try {
            connectDocker();

            if (connectDremioInDocker())
                return;

            if (dremioContainer != null){
                // must delete container as otherwise it will retain that first user created
                dockerClient.removeContainerCmd(dremioContainer.getId()).withForce(true).exec();
                dremioContainer = null;
                logger.debug("Dremio container is removed");
            } else {
                logger.debug("No Dremio container found");
            }

            logger.debug("Pulling Dremio Image...");
            Image image = DockerUtils.checkAndPullImage(dockerClient, DREMIO_DOCKER_IMAGE_NAME, "latest");
            assertNotNull(image);
            logger.debug("Got Dremio image");
            HostConfig hostConfig = HostConfig.newHostConfig()
                    .withPortBindings(DockerUtils.localhostPortBindings(DREMIO_PORTS));
            logger.debug("Creating Dremio container with host config:" + hostConfig);
            dockerClient.createContainerCmd(image.getId())
                    .withName(DREMIO_CONTAINER_NAME)
                    .withExposedPorts(DockerUtils.exposedTcpPorts(DREMIO_PORTS))
                    .withHostConfig(hostConfig)
                    .exec();

            logger.debug("Created Dremio container");
            dremioContainer = DockerUtils.getImageContainers(dockerClient, image).get(0);

            assertNotNull(dremioContainer);
            logger.debug("Got Dremio container:" + dremioContainer + " Starting ...");
            DockerUtils.startRestartContainer(dockerClient, dremioContainer);
            logger.info("Waiting for started Dremio Container: " + DockerUtils.waitContainerRunning(dockerClient, dremioContainer));
            dumpDremioStdin(dremioContainer);

            checkContainers();
            if (!waitForDremioPort(10*60*1000))
                throw new RuntimeException("Dremio failed to startup in time allotted (10 minutes)");

        }catch (Exception e) {
            logger.error("Failed to start Dremio in Docker", e);
            throw new RuntimeException(e);
        }
    }

    static public boolean firstUser() throws Exception {
        return firstUser(10);
    }
    static public boolean firstUser(int maxTries) throws Exception {
        User firstUser = User.builder()
                .name(INTEGRATION_TEST_USERNAME)
                .firstName("Test")
                .lastName("TestLastName")
                .email("foobar@foo.bar")
                .build();
        DremioClientConf conf = DremioRestApp.getDremioConf();

        for (int i  = 0 ; i < maxTries ; i++) {
            try {
                UserAPI.firstUser(conf, firstUser, INTEGRATION_TEST_PASSWORD);
                logger.info("Created first user");
                return true;
            }catch (DremioApiException e){
                if (i%10 == 0) {
                    logger.error("Exception creating first user (try " + i + "): " + e.getCause());
                }
                if (e.getMessage().endsWith("First user can only be created when no user is already registered")){
                    logger.warn("First user already exists");
                    return true;
                }
                Thread.sleep(6000);
            }
        }
        logger.error("Too many attempts failed. Giving up...");
        return false;
    }

    public static Space getTestSpace() {
        return testSpace;
    }

    public static View getZips() {
        return zips;
    }
}

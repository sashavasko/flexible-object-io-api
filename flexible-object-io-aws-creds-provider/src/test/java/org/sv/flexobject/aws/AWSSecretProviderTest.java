package org.sv.flexobject.aws;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.connections.ConnectionManager;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AWSSecretProviderTest {

    @BeforeEach
    public void setUp() {
        AWSSecretProvider.clear();
        clearCredentialProviders();
    }

    @AfterEach
    public void tearDown() {
        AWSSecretProvider.clear();
        clearCredentialProviders();
        System.clearProperty(AWSSecretProvider.AWS_PROFILE_NAME);
        System.clearProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_development");
        System.clearProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_staging");
        System.clearProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_production");
    }

    /**
     * To run this test first do that in terminal :
     * vault login -method=okta username=$USER totp=NUMBERINOCTAVERIFY
     * tmpfile=$(mktemp /tmp/aws-login.XXXXXX)
     * vault read aws/development/<department>/sts/<role> > ${tmpfile}
     * export AWS_ACCESS_KEY_ID=`grep access_key ${tmpfile} | awk '{print $2}'`
     * export AWS_SECRET_ACCESS_KEY=`grep secret_key ${tmpfile} | awk '{print $2}'`
     * export AWS_SESSION_TOKEN=`grep security_token ${tmpfile} | awk '{print $2}'`
     * aws secretsmanager get-secret-value --secret-id arn:aws:secretsmanager:us-east-1:number1:secret:path/to/secret --region us-east-1
     *
     * Use values of AWS env vars to populate credentials in the test
     * Use value of SecretString.password from this last command to populate expected password
     */

    @Disabled
    @Test
    public void testMongoReadonlyForReal() {

        String accessKeyId = "";
        String secretAccessKey = "";
        String sessionToken = "";
        String expectedPassword = "";
        String arn = "arn:aws:secretsmanager:us-east-1:number:secret:path/to/secret";

        if (secretAccessKey.isEmpty() || sessionToken.isEmpty() || expectedPassword.isEmpty())
            throw new RuntimeException("Please fill in the values according to instructions");

        AwsCredentials credentials = AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken);
        AWSSecretProvider provider = new AWSSecretProvider() {
            @Override
            public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
                return StaticCredentialsProvider.create(credentials);
            }
        };

        String secret = provider.getAWSSecret(arn, Region.US_EAST_1, "staging");
        assertEquals(expectedPassword, secret);
    }

    @Test
    public void getSecretUsesConnectionNameDefaultArnDefaultRegionAndTranslatedLevel() {
        TestProvider provider = new TestProvider("plain-secret");
        Properties connectionProperties = new Properties();

        Object secret = provider.getSecret(
                "connectionName",
                ConnectionManager.DeploymentLevel.alpha,
                "environment",
                connectionProperties
        );

        assertEquals("plain-secret", secret);
        assertEquals("connectionName", provider.requestedArn);
        assertEquals(Region.US_EAST_1, provider.requestedRegion);
        assertEquals(AWSSecretProvider.LEVEL_ALPHA, provider.requestedLevel);
    }

    @Test
    public void getSecretUsesConfiguredArnRegionAndLevel() {
        TestProvider provider = new TestProvider("plain-secret");
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty(AWSSecretProvider.ARN, "secret-arn");
        connectionProperties.setProperty(AWSSecretProvider.REGION, "us-west-2");
        connectionProperties.setProperty(AWSSecretProvider.LEVEL, "custom-level");

        Object secret = provider.getSecret(
                "connectionName",
                ConnectionManager.DeploymentLevel.beta,
                "environment",
                connectionProperties
        );

        assertEquals("plain-secret", secret);
        assertEquals("secret-arn", provider.requestedArn);
        assertEquals(Region.US_WEST_2, provider.requestedRegion);
        assertEquals("custom-level", provider.requestedLevel);
    }

    @Test
    public void getSecretTranslatesAllDeploymentLevels() {
        assertEquals(AWSSecretProvider.LEVEL_ALPHA, secretLevelFor(ConnectionManager.DeploymentLevel.alpha));
        assertEquals(AWSSecretProvider.LEVEL_BETA, secretLevelFor(ConnectionManager.DeploymentLevel.beta));
        assertEquals(AWSSecretProvider.LEVEL_PROD, secretLevelFor(ConnectionManager.DeploymentLevel.prod));
    }

    @Test
    public void getSecretReturnsPlainTextWhenSecretIsNotJson() {
        TestProvider provider = new TestProvider("not-json");
        Properties connectionProperties = new Properties();

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, connectionProperties);

        assertEquals("not-json", secret);
        assertTrue(connectionProperties.isEmpty());
    }

    @Test
    public void getSecretReturnsJsonValueNodeText() {
        TestProvider provider = new TestProvider("\"json-secret\"");

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, new Properties());

        assertEquals("json-secret", secret);
    }

    @Test
    public void getSecretExtractsPasswordAndUsernameAliasesFromJsonObject() {
        TestProvider provider = new TestProvider("{\"username\":\"db-user\",\"password\":\"db-password\"}");
        Properties connectionProperties = new Properties();

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, connectionProperties);

        assertEquals("db-password", secret);
        assertEquals("db-user", connectionProperties.getProperty("user"));
        assertEquals("db-user", connectionProperties.getProperty("username"));
        assertEquals("db-user", connectionProperties.getProperty("userName"));
    }

    @Test
    public void getSecretExtractsPasswordFromSecretStringWrapper() {
        TestProvider provider = new TestProvider("{\"SecretString\":{\"username\":\"db-user\",\"password\":\"db-password\"}}");
        Properties connectionProperties = new Properties();

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, connectionProperties);

        assertEquals("db-password", secret);
        assertEquals("db-user", connectionProperties.getProperty("user"));
    }

    @Test
    public void getSecretReturnsOriginalJsonWhenJsonObjectHasNoPassword() {
        String secretJson = "{\"username\":\"db-user\"}";
        TestProvider provider = new TestProvider(secretJson);
        Properties connectionProperties = new Properties();

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, connectionProperties);

        assertEquals(secretJson, secret);
        assertEquals("db-user", connectionProperties.getProperty("user"));
    }

    @Test
    public void getSecretReturnsNullWhenAwsReturnsNullSecret() {
        TestProvider provider = new TestProvider(null);

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, null, new Properties());

        assertNull(secret);
    }

    @Test
    public void getAWSSecretReturnsSecretString() {
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        when(client.getSecretValue(any(GetSecretValueRequest.class))).thenReturn(
                GetSecretValueResponse.builder().secretString("secret-value").build()
        );
        ClientProvider provider = new ClientProvider(client);

        String secret = provider.getAWSSecret("secret-arn", Region.US_EAST_1, AWSSecretProvider.LEVEL_ALPHA);

        assertEquals("secret-value", secret);
        verify(client).getSecretValue(argThat((GetSecretValueRequest request) -> "secret-arn".equals(request.secretId())));
    }

    @Test
    public void getAWSSecretReturnsNullWhenSecretDoesNotExist() {
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        when(client.getSecretValue(any(GetSecretValueRequest.class))).thenThrow(ResourceNotFoundException.builder().build());
        ClientProvider provider = new ClientProvider(client);

        String secret = provider.getAWSSecret("secret-arn", Region.US_EAST_1, AWSSecretProvider.LEVEL_ALPHA);

        assertNull(secret);
    }

    @Test
    public void getAWSSecretWrapsSecretsManagerException() {
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        SecretsManagerException cause = mock(SecretsManagerException.class);
        when(client.getSecretValue(any(GetSecretValueRequest.class))).thenThrow(cause);
        ClientProvider provider = new ClientProvider(client);

        RuntimeException exception = assertThrows(
                RuntimeException.class,
                () -> provider.getAWSSecret("secret-arn", Region.US_EAST_1, AWSSecretProvider.LEVEL_ALPHA)
        );

        assertEquals("Failed to load password from AWS secret-arn", exception.getMessage());
        assertSame(cause, exception.getCause());
    }

    @Test
    public void getClientCachesClientsByRegionAndLevel() {
        CountingCredentialProvider provider = new CountingCredentialProvider();

        SecretsManagerClient first = provider.getClient(Region.US_EAST_1, "cache-level-one");
        SecretsManagerClient second = provider.getClient(Region.US_EAST_1, "cache-level-one");
        SecretsManagerClient third = provider.getClient(Region.US_WEST_2, "cache-level-one");
        SecretsManagerClient fourth = provider.getClient(Region.US_EAST_1, "cache-level-two");

        assertSame(first, second);
        assertNotSame(first, third);
        assertNotSame(first, fourth);
        assertEquals(2, provider.credentialsProviderCalls);
    }

    @Test
    public void getCachedCredentialProviderCachesByLevel() {
        CountingCredentialProvider provider = new CountingCredentialProvider();

        AwsCredentialsProvider first = provider.getCachedCredentialProvider("cached-level");
        AwsCredentialsProvider second = provider.getCachedCredentialProvider("cached-level");
        AwsCredentialsProvider third = provider.getCachedCredentialProvider("another-cached-level");

        assertSame(first, second);
        assertNotSame(first, third);
        assertEquals(2, provider.credentialsProviderCalls);
    }

    @Test
    public void clearClosesCachedClients() throws Exception {
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        cachedClients().put("mock-region:close-level", client);

        AWSSecretProvider.clear();

        verify(client).close();
        assertTrue(cachedClients().isEmpty());
    }

    @Test
    public void closeClearsCachedClients() throws Exception {
        TestProvider provider = new TestProvider("secret");
        SecretsManagerClient client = mock(SecretsManagerClient.class);
        cachedClients().put("mock-region:close-via-provider-level", client);

        provider.close();

        verify(client).close();
        assertTrue(cachedClients().isEmpty());
    }

    @Test
    public void getPropertiesReturnsNull() {
        TestProvider provider = new TestProvider("secret");

        assertNull(provider.getProperties("connectionName", ConnectionManager.DeploymentLevel.alpha, "environment"));
    }

    @Test
    public void envCredentialsProviderCreatesProvider() {
        AWSSecretProvider.AWSSecretProviderWithEnvCredentials provider = new AWSSecretProvider.AWSSecretProviderWithEnvCredentials();

        assertNotNull(provider.getCredentialsProvider(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameUsesExplicitProfileBeforeLevelAndSystemProperties() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider = new AWSSecretProvider.AWSSecretProviderWithProfile()
                .setProfileName(AWSSecretProvider.LEVEL_ALPHA, "alpha-profile")
                .setProfileName("explicit-profile");
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_development", "system-alpha-profile");
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME, "system-default-profile");

        assertEquals("explicit-profile", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameUsesLevelSpecificProfileBeforeSystemProperties() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider = new AWSSecretProvider.AWSSecretProviderWithProfile()
                .setProfileName(AWSSecretProvider.LEVEL_ALPHA, "alpha-profile");
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_development", "system-alpha-profile");
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME, "system-default-profile");

        assertEquals("alpha-profile", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameUsesLevelSpecificSystemPropertyBeforeDefaultSystemProperty() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider = new AWSSecretProvider.AWSSecretProviderWithProfile();
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME + "_development", "system-alpha-profile");
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME, "system-default-profile");

        assertEquals("system-alpha-profile", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameUsesDefaultSystemProperty() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider = new AWSSecretProvider.AWSSecretProviderWithProfile();
        System.setProperty(AWSSecretProvider.AWS_PROFILE_NAME, "system-default-profile");

        assertEquals("system-default-profile", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameCanComeFromConstructor() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider =
                new AWSSecretProvider.AWSSecretProviderWithProfile("constructor-profile");

        assertEquals("constructor-profile", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void profileNameReturnsNullWhenNoProfileIsConfigured() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider = new AWSSecretProvider.AWSSecretProviderWithProfile();

        assertNull(provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
    }

    @Test
    public void createForTestConfiguresTestProfileNames() {
        AWSSecretProvider.AWSSecretProviderWithProfile provider =
                (AWSSecretProvider.AWSSecretProviderWithProfile) AWSSecretProvider.createForTest();

        assertEquals("test-development", provider.getProfileName(AWSSecretProvider.LEVEL_ALPHA));
        assertEquals("test-staging", provider.getProfileName(AWSSecretProvider.LEVEL_BETA));
        assertEquals("test-production", provider.getProfileName(AWSSecretProvider.LEVEL_PROD));
    }

    private String secretLevelFor(ConnectionManager.DeploymentLevel deploymentLevel) {
        TestProvider provider = new TestProvider("secret");
        provider.getSecret("connectionName", deploymentLevel, null, new Properties());
        return provider.requestedLevel;
    }

    @SuppressWarnings("unchecked")
    private Map<String, SecretsManagerClient> cachedClients() throws Exception {
        Field clients = AWSSecretProvider.class.getDeclaredField("clients");
        clients.setAccessible(true);
        return (Map<String, SecretsManagerClient>) clients.get(null);
    }

    private void clearCredentialProviders() {
        try {
            Field credentialProviders = AWSSecretProvider.class.getDeclaredField("credentialProviders");
            credentialProviders.setAccessible(true);
            ((Map<?, ?>) credentialProviders.get(null)).clear();
        } catch (Exception e) {
            throw new RuntimeException("Failed to clear cached credential providers", e);
        }
    }

    static class TestProvider extends AWSSecretProvider {
        private final String secret;
        String requestedArn;
        Region requestedRegion;
        String requestedLevel;

        TestProvider(String secret) {
            this.secret = secret;
        }

        @Override
        public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));
        }

        @Override
        public String getAWSSecret(String arn, Region region, String level) {
            requestedArn = arn;
            requestedRegion = region;
            requestedLevel = level;
            return secret;
        }
    }

    static class ClientProvider extends AWSSecretProvider {
        private final SecretsManagerClient client;

        ClientProvider(SecretsManagerClient client) {
            this.client = client;
        }

        @Override
        public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
            return StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey"));
        }

        @Override
        public SecretsManagerClient getClient(Region region, String level) {
            return client;
        }
    }

    static class CountingCredentialProvider extends AWSSecretProvider {
        int credentialsProviderCalls;

        @Override
        public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
            credentialsProviderCalls++;
            return StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("accessKey-" + awsLevel, "secretKey-" + awsLevel)
            );
        }
    }
}

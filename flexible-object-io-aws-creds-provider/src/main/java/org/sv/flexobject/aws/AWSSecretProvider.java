package org.sv.flexobject.aws;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.SecretProvider;
import org.sv.flexobject.json.MapperFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class AWSSecretProvider implements SecretProvider, AutoCloseable {

    public static final String ARN = "arn";
    public static final String REGION = "region";
    public static final String LEVEL = "level";
    public static final String DEFAULT_REGION = Region.US_EAST_1.toString();
    public static final String LEVEL_ALPHA = "development";
    public static final String LEVEL_BETA = "staging";
    public static final String LEVEL_PROD = "production";
    public static final String AWS_PROFILE_NAME = "aws_profile_name";

    public abstract AwsCredentialsProvider getCredentialsProvider(String awsLevel);
    private static final Map<String, AwsCredentialsProvider> credentialProviders = new HashMap<>();
    private static final Map<String, SecretsManagerClient> clients = new HashMap<>();

    public static class AWSSecretProviderWithEnvCredentials extends AWSSecretProvider{

        @Override
        public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
            return EnvironmentVariableCredentialsProvider.create();
        }
    }

//    public static boolean checkProfile(String profileName) {
//        GetCallerIdentityRequest request = GetCallerIdentityRequest.builder()..build();
//        StsClient.create().getCallerIdentity(request);
//    }

    public static class AWSSecretProviderWithProfile extends AWSSecretProvider{
        Map<String, String> profileNames = new HashMap<>();
        String profileName;

        public AWSSecretProviderWithProfile(String profileName) {
            this.profileName = profileName;
        }

        public AWSSecretProviderWithProfile() {
        }

        public AWSSecretProviderWithProfile setProfileName(String awsLevel, String profileName){
            profileNames.put(awsLevel, profileName);
            return this;
        }

        public AWSSecretProviderWithProfile setProfileName(String profileName){
            this.profileName = profileName;
            return this;
        }

        public String getProfileName(String awsLevel) {
            if (profileName != null)
                return profileName;

            if (profileNames.containsKey(awsLevel))
                return profileNames.get(awsLevel);

            String levelSpecificProfileName = System.getProperty(AWS_PROFILE_NAME + "_" + awsLevel);
            if (StringUtils.isNotBlank(levelSpecificProfileName))
                return levelSpecificProfileName;

            return System.getProperty(AWS_PROFILE_NAME);
        }

        @Override
        public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
            ProfileCredentialsProvider.Builder builder = ProfileCredentialsProvider.builder();

            String profileName = getProfileName(awsLevel);
            if (!StringUtils.isEmpty(profileName))
                builder.profileName(profileName);

            AwsCredentialsProvider credentialsProvider = builder.build();


            try {
                credentialsProvider.resolveCredentials();
            } catch (SdkClientException e){
                if (e.getMessage().toLowerCase().contains("token") || e.getMessage().contains("SSO")){
                    System.err.println("Please login into AWS using command : \"aws sso login --profile " + profileName + "\"");
                } else {
                    System.err.println("Failed to resolve AWS credentials using profile " + profileName + ". \nPlease add appropriate profile to ~/.aws/config by either copy-pasting existing profile with this name or re-running \"aws configure sso\"\n\n\n");
                }
                throw e;
            }
            return credentialsProvider;
        }
    }



    public static AWSSecretProvider createForTest(){
        return new AWSSecretProvider.AWSSecretProviderWithProfile()
                .setProfileName(AWSSecretProvider.LEVEL_ALPHA, "test-development")
                .setProfileName(AWSSecretProvider.LEVEL_BETA, "test-staging")
                .setProfileName(AWSSecretProvider.LEVEL_PROD, "test-production")
                ;
    }

    public SecretsManagerClient getClient (Region region, String level){
        String clientKey = region.toString() + ":" + level;

        if (!clients.containsKey(clientKey)){
            synchronized (clients){
                if (!clients.containsKey(clientKey)) {
                    AwsCredentialsProvider credentialsProvider = getCachedCredentialProvider(level);

                    SecretsManagerClient secretsClient = SecretsManagerClient.builder()
                            .region(region)
                            .credentialsProvider(credentialsProvider)
                            .build();
                    clients.put(clientKey, secretsClient);
                    return secretsClient;
                }
            }
        }
        return clients.get(clientKey);
    }

    public AwsCredentialsProvider getCachedCredentialProvider(String level) {
        if (credentialProviders.containsKey(level))
            return credentialProviders.get(level);
        AwsCredentialsProvider credentialsProvider = getCredentialsProvider(level);
        credentialProviders.put(level, credentialsProvider);

        return credentialsProvider;
    }

    public static void clear(){
        for (SecretsManagerClient client : clients.values())
            client.close();
        clients.clear();
    }

    private Object handleSecretWithProperties(JsonNode secretJson, Properties connectionProperties) {
        if (secretJson.isValueNode()){
            return secretJson.asText();
        } else {
            if (secretJson.has("username")) {
                String username = secretJson.get("username").asText();
                connectionProperties.setProperty("user", username);
                connectionProperties.setProperty("username", username);
                connectionProperties.setProperty("userName", username);
            }
            if (!secretJson.has("password"))
                throw new RuntimeException("No password found in a SecretString object of the secret");
            return secretJson.get("password").asText();
        }
    }
    private Object handleSecretWithProperties(String secret, Properties connectionProperties) {
        try{
            JsonNode jsonSecret = MapperFactory.getObjectReader().readTree(secret);
            if (jsonSecret.has("SecretString")){
                return handleSecretWithProperties(jsonSecret.get("SecretString"), connectionProperties);
            }
            return handleSecretWithProperties(jsonSecret, connectionProperties);
        } catch (Exception e) {
        }
        return secret;
    }

    private String translateLevel(ConnectionManager.DeploymentLevel deploymentLevel){
        switch (deploymentLevel){
            case alpha: return LEVEL_ALPHA;
            case beta: return LEVEL_BETA;
            case prod: return LEVEL_PROD;
        }
        return null;
    }

    public String getAWSSecret(String arn, Region region, String level) {
        SecretsManagerClient secretsClient = getClient(region, level);

        try {
            GetSecretValueRequest valueRequest = GetSecretValueRequest.builder()
                    .secretId(arn)
                    .build();

            GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
            String secret = valueResponse.secretString();
            return secret;

        } catch (ResourceNotFoundException e){
//            logger.error("Cannot find password for arn " + arn);
            return null;
        } catch (SecretsManagerException e) {
            throw new RuntimeException("Failed to load password from AWS " + arn, e);
        }
    }

    @Override
    public Object getSecret(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment, Properties connectionProperties) {
        String arn = connectionProperties.getProperty(ARN, connectionName);
        String region = connectionProperties.getProperty(REGION, DEFAULT_REGION);
        String level = connectionProperties.getProperty(LEVEL, translateLevel(deploymentLevel));
        String secret = getAWSSecret(arn, Region.of(region), level);
        return handleSecretWithProperties(secret, connectionProperties);
    }

    @Override
    public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        return null;
    }

    @Override
    public void close() {
        clear();
    }

}

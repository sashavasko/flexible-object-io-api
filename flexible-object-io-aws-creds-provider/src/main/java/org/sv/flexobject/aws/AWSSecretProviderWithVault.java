package org.sv.flexobject.aws;

import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LogicalResponse;
import com.bettercloud.vault.rest.RestResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Map;

public class AWSSecretProviderWithVault extends AWSSecretProvider{
    public static final Logger logger = LogManager.getLogger(AWSSecretProviderWithVault.class);
    private static final String DEFAULT_HTTPS_VAULT_URL = "https://vault.operations.production.aws.COMPANY.io";
    public static final String ACCESS_KEY = "access_key";
    public static final String SECRET_KEY = "secret_key";
    public static final String SECURITY_TOKEN = "security_token";
    private static final int ENGINE_VERSION = 1;

    private String awsVaultSecretKey; // for example aws/development/DEPARTMENT/sts/ROLE
    private String team;
    private String role;
    private String vaultRoleId;
    private String vaultRoleSecretId;
    private String vaultRoleJWT;
    private String vaultRolePath = "approle";

    private String token;

    public static class Builder {
        private String awsVaultSecretKey; // for example aws/development/DEPARTMENT/sts/ROLE
        private String team;
        private String role;
        private String token;
        private String vaultRoleId;
        private String vaultRoleSecretId;
        private String vaultRoleJWT;
        private String vaultRolePath;

        public Builder() {
        }
        public Builder(Map<String, String> env) {
            token(env.get("VAULT_TOKEN"));
            key(env.get("VAULT_SECRET_KEY"));
            vaultRoleId(env.get("VAULT_APP_ROLE"));
            vaultRoleSecretId(env.get("VAULT_APP_ROLE_SECRET"));
            vaultRoleJWT(env.get("VAULT_JWT"));
            vaultRolePath(env.get("VAULT_ROLE_PATH"));
            team(env.get("VAULT_TEAM"));
            role(env.get("VAULT_ROLE"));
        }

        public Builder key(String awsVaultSecretKey){
            this.awsVaultSecretKey = awsVaultSecretKey;
            return this;
        }

        public Builder team(String team){
            this.team = team;
            return this;
        }

        public Builder role(String role) {
            this.role = role;
            return this;
        }

        public Builder vaultRoleId(String vaultRoleId) {
            this.vaultRoleId = vaultRoleId;
            return this;
        }
        public Builder vaultRoleSecretId(String vaultRoleSecretId) {
            this.vaultRoleSecretId = vaultRoleSecretId;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }
        public Builder vaultRoleJWT(String vaultRoleJWT) {
            this.vaultRoleJWT = vaultRoleJWT;
            return this;
        }

        public Builder vaultRolePath(String vaultRolePath) {
            this.vaultRolePath = vaultRolePath;
            return this;
        }

        public AWSSecretProviderWithVault build() {
            return build(new AWSSecretProviderWithVault());
        }

        public AWSSecretProviderWithVault build(AWSSecretProviderWithVault provider){
            provider.team = team;
            provider.awsVaultSecretKey = awsVaultSecretKey;
            provider.role = role;
            provider.token = token;
            provider.vaultRoleId = vaultRoleId;
            provider.vaultRoleSecretId = vaultRoleSecretId;
            return provider;
        }
    }
    public AWSSecretProviderWithVault() {
        Builder builder = new Builder(System.getenv());
        builder.build(this);
    }

    public static Builder builder(){
        return new Builder();
    }

    protected String getVaultSecretKey(String awsLevel){
        if (awsVaultSecretKey != null)
            return awsVaultSecretKey;
        return "aws/" + awsLevel + "/" + team + "/sts/" + role;
    }

    private VaultConfig getBasicVaultConfiguration() throws VaultException {
        VaultConfig vaultConfig = new VaultConfig()
                .address(DEFAULT_HTTPS_VAULT_URL)
                .engineVersion(ENGINE_VERSION);
        return vaultConfig.build();
    }

    private VaultConfig getVaultConfiguration() throws VaultException {
        VaultConfig vaultConfig = new VaultConfig()
                 .address(DEFAULT_HTTPS_VAULT_URL)
                 .engineVersion(ENGINE_VERSION);
        if (StringUtils.isBlank(token)
                && StringUtils.isNotBlank(vaultRoleId)) {
            if (!vaultAppRoleAuth())
                throw new RuntimeException("Failed to authenticate to Vault with provided app role and secret IDs");
        }
        if (StringUtils.isNotBlank(token))
            vaultConfig.token(token);
        return vaultConfig.build();
    }

    protected Vault getVault() throws VaultException {
        return getVault(getVaultConfiguration());
    }
    protected Vault getVault(VaultConfig config) throws VaultException {
        return new Vault(config, ENGINE_VERSION);
    }
    protected Map<String, String> queryVault(String key) throws VaultException {
        logger.debug("querying vault for key:" + key);
        LogicalResponse response = getVault().logical()
                .read(key);
        RestResponse restResponse = response.getRestResponse();
        if (restResponse.getStatus() != 200){
            throw new RuntimeException("REST failed with code:" + restResponse.getStatus() + " and body:" + new String(restResponse.getBody()));
        }
        return response.getData();
    }

    protected boolean vaultAppRoleAuth() throws VaultException {
        String roleId = getRoleId();
        String secretId = getRoleSecretId();
        if (software.amazon.awssdk.utils.StringUtils.isNotBlank(roleId)){
            AuthResponse response = null;
            if (StringUtils.isNotBlank(secretId)) {
                logger.debug(String.format("loginByAppRole(%s, %s, %s)", vaultRolePath, roleId, secretId));
                response = getVault(getBasicVaultConfiguration()).auth().loginByAppRole(vaultRolePath, roleId, secretId);
            } else if (StringUtils.isNotBlank(vaultRoleJWT)) {
                logger.debug(String.format("loginByJwt(%s, %s, %s)", vaultRolePath, roleId, vaultRoleJWT));
                response = getVault(getBasicVaultConfiguration()).auth().loginByJwt(vaultRolePath, roleId, vaultRoleJWT);
            }
            if (response == null)
                return false;

            RestResponse restResponse = response.getRestResponse();
            if (restResponse.getStatus() != 200){
                throw new RuntimeException("REST failed with code : " + restResponse.getStatus() + " and body: " + new String(restResponse.getBody()));
            }
            this.token = response.getAuthClientToken();
            logger.debug("Vault token:" + token);
            return software.amazon.awssdk.utils.StringUtils.isNotBlank(this.token);
        }
        return false;
    }


    public Map<String,String> getAWSCredentialsFromVault(String awsLevel) {
        try {
            String vaultSecretKey = getVaultSecretKey(awsLevel);
            Map<String, String> vaultCreds = queryVault(vaultSecretKey);

            if (vaultCreds.isEmpty()){
                if (vaultAppRoleAuth()){
                    vaultCreds = queryVault(vaultSecretKey);
                }
            }

            if (vaultCreds.isEmpty())
                throw new RuntimeException("Failed to read AWS credentials from Vault. Please login using: \n   vault login -address=" + DEFAULT_HTTPS_VAULT_URL + " -method=okta username=$USER totp=NUMBERINOCTAVERIFY\n");

            return vaultCreds;
        } catch (VaultException e) {
            throw new RuntimeException("Failed to read AWS access credentials from Vault", e);
        }
    }

    private String getRoleSecretId() {
        if (StringUtils.isNotBlank(vaultRoleSecretId))
            return vaultRoleSecretId;
        return null;
    }

    private String getRoleId() {
        if (StringUtils.isNotBlank(vaultRoleId))
            return vaultRoleId;
        return null;
    }

    @Override
    public AwsCredentialsProvider getCredentialsProvider(String awsLevel) {
        Map<String, String> vaultCreds = getAWSCredentialsFromVault(awsLevel);

        AwsCredentials credentials = AwsSessionCredentials.create(
                vaultCreds.get(ACCESS_KEY),
                vaultCreds.get(SECRET_KEY),
                vaultCreds.get(SECURITY_TOKEN));

        return StaticCredentialsProvider.create(credentials);
    }

    public String getToken() {
        return token;
    }
}

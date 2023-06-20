package org.sv.flexobject.aws;

import com.bettercloud.vault.VaultException;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.sv.flexobject.connections.ConnectionManager;

import java.util.Properties;

import static org.junit.Assert.*;

public class AWSSecretProviderWithVaultTest {

    @Ignore
    @Test
    public void getVhrAdeAlphaCreds() throws JsonProcessingException {
        AWSSecretProviderWithVault provider = new AWSSecretProviderWithVault();
        String arn="arn:aws:secretsmanager:us-east-1:number:secret:path/to/secret";

        Properties connectionProperties = new Properties();
        connectionProperties.put("arn", arn);

        Object secret = provider.getSecret("connectionName", ConnectionManager.DeploymentLevel.alpha, "test", connectionProperties);
        assertTrue(connectionProperties.containsKey("username"));
        assertTrue(connectionProperties.containsKey("user"));
        assertTrue(StringUtils.isNotBlank((String) secret));
    }


    @Ignore
    @Test
    public void vaultAppRoleAuth() throws VaultException {
        AWSSecretProviderWithVault provider = AWSSecretProviderWithVault.builder()
                .vaultRoleId("")        // value of ROLE_ID
                .vaultRoleSecretId("")  // value of ROLE_SECRET_ID
                .build();
        assertTrue(provider.vaultAppRoleAuth());
        System.out.println(provider.getToken());
        String vaultKey = "aws/production/department/sts/role";
        System.out.println(provider.queryVault(vaultKey));
    }
}
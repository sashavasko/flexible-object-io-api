package org.sv.flexobject.dremio.api;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.aws.AWSSecretProviderWithVault;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;
import org.sv.flexobject.connections.SecretProvider;
import org.sv.flexobject.dremio.DremioClientConf;
import org.sv.flexobject.dremio.DremioClientProvider;
import org.sv.flexobject.dremio.DremioRestApp;
import org.sv.flexobject.properties.FilePropertiesProvider;

import java.util.Properties;

import static org.junit.Assert.*;

public class AuthAPITest {

    @Test
    public void authenticate() throws Exception {
        Session session = DremioRestApp.getClient().getSession();
        AuthAPI auth = new AuthAPI(session);
        String secret = DremioRestApp.INTEGRATION_TEST_PASSWORD;
        String token = auth.authenticate(secret);
        System.out.println(token);
        assertTrue(StringUtils.isNotBlank(token));
    }
}
package org.sv.flexobject.connections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class ConnectionManagerTest {

    public static class FakeConnection implements AutoCloseable {

        String name;

        public String getName() {
            return name;
        }

        public FakeConnection setName(String name) {
            this.name = name;
            return this;
        }

        @Override
        public void close() throws Exception {

        }
    }

    public static class FakeConnectionProvider implements ConnectionProvider{

        @Override
        public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
            return new FakeConnection().setName(name);
        }

        @Override
        public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
            return Arrays.asList(FakeConnection.class);
        }
    }

    @Mock
    ConnectionProvider mockConnectionProvider;
    @Mock
    ConnectionProvider mockConnectionProvider2;

    @Mock
    PropertiesProvider mockPropertiesProvider;
    @Mock
    PropertiesProvider mockPropertiesProvider2;
    @Mock
    PropertiesProvider mockPropertiesProvider3;

    @Mock
    SecretProvider mockSecretProvider;
    @Mock
    SecretProvider mockSecretProvider2;
    @Mock
    SecretProvider mockSecretProvider3;

    @Mock
    Object mockSecret;
    @Mock
    Object mockSecret2;
    @Mock
    Object mockSecret3;


    Properties mockProperties = new Properties();

    Properties mockProperties2 = new Properties();

    Properties mockProperties3 = new Properties();

    @Mock
    FakeConnection mockConnection;
    @Mock
    FakeConnection mockConnection2;
    @Mock
    FakeConnection mockConnection3;
    String connectionName = "fooConn";

    @Before
    public void setUp() throws Exception {
        mockProperties.setProperty("foo", "bar");
        mockProperties2.setProperty("foo2", "bar2");
        mockProperties3.setProperty("foo3", "bar3");
        ConnectionManager.getInstance()
                .clearAll()
                .registerProvider(mockConnectionProvider, FakeConnection.class)
                .registerPropertiesProvider(mockPropertiesProvider)
                .registerSecretProvider(mockSecretProvider)
                .setDeploymentLevel(ConnectionManager.DeploymentLevel.alpha)
                .setEnvironment("unitTest");
        when(mockSecretProvider.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", mockProperties)).thenReturn(mockSecret);
        when(mockSecretProvider.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", mockProperties2)).thenReturn(mockSecret);
        when(mockSecretProvider.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", mockProperties3)).thenReturn(mockSecret);
        when(mockSecretProvider.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", null)).thenReturn(mockSecret);
        when(mockPropertiesProvider.getProperties(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(mockProperties);
        when(mockConnectionProvider.getConnection(connectionName, mockProperties, mockSecret)).thenReturn(mockConnection);
        when(mockConnectionProvider.requiresProperties()).thenReturn(true);
        when(mockConnectionProvider2.requiresProperties()).thenReturn(true);
    }

    @After
    public void tearDown() throws Exception {
        ConnectionManager.getInstance().clearAll();
    }

    @Test
    public void setDeploymentLevel() {
        assertEquals(ConnectionManager.DeploymentLevel.alpha, ConnectionManager.getInstance().getDeploymentLevel());

        ConnectionManager.getInstance().setDeploymentLevel(ConnectionManager.DeploymentLevel.beta);
        assertEquals(ConnectionManager.DeploymentLevel.beta, ConnectionManager.getInstance().getDeploymentLevel());

        ConnectionManager.getInstance().setDeploymentLevel(ConnectionManager.DeploymentLevel.prod);
        assertEquals(ConnectionManager.DeploymentLevel.prod, ConnectionManager.getInstance().getDeploymentLevel());
    }

    @Test
    public void setGetEnvironment() {
        ConnectionManager.getInstance().setEnvironment("foo");
        assertEquals("foo", ConnectionManager.getInstance().getEnvironment());

        ConnectionManager.getInstance().setEnvironment("bar");
        assertEquals("bar", ConnectionManager.getInstance().getEnvironment());
    }

    @Test
    public void getConnection() throws Exception {
        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));
    }

    @Test
    public void getConnectionWithOverrides() throws Exception {
        Properties overrides =new Properties();
        overrides.setProperty("foo", "barOver");

        Properties expectedFullProperties = new Properties();
        expectedFullProperties.putAll(mockProperties);
        expectedFullProperties.putAll(overrides);

        when(mockConnectionProvider.getConnection(connectionName, expectedFullProperties, null)).thenReturn(mockConnection);
        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName, overrides));
    }

    @Test(expected = IOException.class)
    public void getConnectionForUnknownClassShouldThrow() throws Exception {
        ConnectionManager.getInstance().getConnection(AutoCloseable.class, connectionName);
    }

    @Test
    public void registerProviderForMultipleClasses() throws Exception {
        ConnectionManager.getInstance()
                .registerProvider(mockConnectionProvider2, FakeConnection.class, AutoCloseable.class);
        when(mockConnectionProvider2.getConnection(connectionName, mockProperties, mockSecret)).thenReturn(mockConnection2);
        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(AutoCloseable.class, connectionName));
    }

    @Test(expected = IOException.class)
    public void unregisterProvider() throws Exception {
        ConnectionManager.getInstance()
                .unregisterProvider(mockConnectionProvider)
                .getConnection(FakeConnection.class, connectionName);
    }

    @Test
    public void registerAnotherProvider() throws Exception {
        assertEquals(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        when(mockConnectionProvider2.getConnection(connectionName, mockProperties, mockSecret)).thenReturn(mockConnection2);
        ConnectionManager.getInstance().registerProvider(mockConnectionProvider2, FakeConnection.class);

        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));
    }

    @Test
    public void registerProviderByClass() throws Exception {
        ConnectionManager.getInstance().registerProvider(FakeConnectionProvider.class);
        when(mockPropertiesProvider.getProperties("connectionRegisteredByClass", ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(mockProperties);

        FakeConnection connection = (FakeConnection) ConnectionManager.getInstance().getConnection(FakeConnection.class, "connectionRegisteredByClass");
        assertEquals(connection.getName(), "connectionRegisteredByClass");
    }

    @Test
    public void unregisterProviderByClass() throws Exception {
        ConnectionManager.getInstance().registerProvider(FakeConnectionProvider.class);

        when(mockPropertiesProvider.getProperties("connectionUnRegisteredByClass", ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(mockProperties);

        FakeConnection connection = (FakeConnection) ConnectionManager.getInstance().getConnection(FakeConnection.class, "connectionUnRegisteredByClass");
        assertEquals(connection.getName(), "connectionUnRegisteredByClass");

        ConnectionManager.getInstance().unregisterProvider(FakeConnectionProvider.class);
        try {
            ConnectionManager.getInstance().getConnection(FakeConnection.class, "connectionUnRegisteredByClass");
            throw new RuntimeException("Should have thrown");
        }catch(IOException e){
            assertEquals("Unknown connection provider for class org.sv.flexobject.connections.ConnectionManagerTest$FakeConnection named connectionUnRegisteredByClass", e.getMessage());
        }
    }

    @Test
    public void unregisterPropertiesProvider() throws Exception {
        ConnectionManager.getInstance()
                .registerPropertiesProvider(mockPropertiesProvider2)
                .registerPropertiesProvider(mockPropertiesProvider3);

        when(mockPropertiesProvider2.getProperties(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(mockProperties2);
        when(mockPropertiesProvider3.getProperties(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(mockProperties3);
        when(mockConnectionProvider.getConnection(connectionName, mockProperties2, mockSecret)).thenReturn(mockConnection2);
        when(mockConnectionProvider.getConnection(connectionName, mockProperties3, mockSecret)).thenReturn(mockConnection3);

//        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterPropertiesProvider(mockPropertiesProvider);

        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().registerPropertiesProvider(mockPropertiesProvider);

        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterPropertiesProvider(mockPropertiesProvider2);

        assertSame(mockConnection3, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterPropertiesProvider(mockPropertiesProvider3);

        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        when(mockPropertiesProvider.getProperties(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest")).thenReturn(null);

        try {
            ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName);
            throw new RuntimeException("Must have thrown an IOException");
        }catch(IOException e){
        }

        ConnectionManager.getInstance().unregisterPropertiesProvider(mockPropertiesProvider);

        try{
            ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName);
            throw new RuntimeException("Must have thrown an IOException");
        }catch(IOException e){
        }
    }

    @Test
    public void unregisterSecretProvider() throws Exception {
        ConnectionManager.getInstance()
                .registerSecretProvider(mockSecretProvider2)
                .registerSecretProvider(mockSecretProvider3);

        when(mockSecretProvider2.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", mockProperties)).thenReturn(mockSecret2);
        when(mockSecretProvider3.getSecret(connectionName, ConnectionManager.DeploymentLevel.alpha, "unitTest", mockProperties)).thenReturn(mockSecret3);
        when(mockConnectionProvider.getConnection(connectionName, mockProperties, mockSecret2)).thenReturn(mockConnection2);
        when(mockConnectionProvider.getConnection(connectionName, mockProperties, mockSecret3)).thenReturn(mockConnection3);

        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterSecretProvider(mockSecretProvider);

        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().registerSecretProvider(mockSecretProvider);

        assertSame(mockConnection2, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterSecretProvider(mockSecretProvider2);

        assertSame(mockConnection3, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));

        ConnectionManager.getInstance().unregisterSecretProvider(mockSecretProvider3);

        assertSame(mockConnection, ConnectionManager.getInstance().getConnection(FakeConnection.class, connectionName));
    }
}
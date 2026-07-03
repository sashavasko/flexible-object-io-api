package org.sv.flexobject.sql.dao;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.adapter.AdapterFactory;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.connections.PropertiesProvider;
import org.sv.flexobject.sql.SqlInputAdapter;
import org.sv.flexobject.sql.SqlOutAdapter;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ConnectionDaoTest {

    @Mock
    PropertiesProvider mockPropertiesProvider;

    @Mock
    ConnectionProvider mockProvider;

    Properties mockProperties1 = new Properties();

    Properties mockProperties2 = new Properties();

    @Mock
    Connection connection;

    @Mock
    Connection connection2;

    @Mock
    AdapterFactory adapterFactory;

    @Mock
    InAdapter inAdapter;

    @Mock
    OutAdapter outAdapter;

    @BeforeEach
    public void setUp() throws Exception {
        mockProperties1.put("foo1", "bar1");
        mockProperties2.put("foo2", "bar2");
        ConnectionManager.getInstance().registerProvider(mockProvider, Connection.class);
        ConnectionManager.getInstance().registerPropertiesProvider(mockPropertiesProvider);
        ConnectionManager.getInstance().setEnvironment("unitTest");

        when(mockPropertiesProvider.getProperties("test", ConnectionManager.DeploymentLevel.alpha, "unitTest"))
                .thenReturn(mockProperties1);
        when(mockPropertiesProvider.getProperties("test2", ConnectionManager.DeploymentLevel.alpha, "unitTest"))
                .thenReturn(mockProperties2);
        when(mockProvider.getConnection("test", Connection.class, mockProperties1, null)).thenReturn(connection);
        when(mockProvider.getConnection("test2", Connection.class, mockProperties2, null)).thenReturn(connection2);
        when(adapterFactory.createInputAdapter("input")).thenReturn(inAdapter);
        when(adapterFactory.createOutputAdapter("output")).thenReturn(outAdapter);
    }

    @AfterEach
    public void tearDown() throws Exception {
        ConnectionManager.getInstance().clearAll();
    }

    @Test
    public void connectionLifecycle() throws Exception {
        ConnectionDao dao = new ConnectionDao("test");
        assertSame(connection, dao.getConnection());
        dao.getConnection();

        dao.close();
        Mockito.verify(connection).close();

        dao.getConnection();
        Mockito.verify(mockProvider, Mockito.times(2)).getConnection("test", Connection.class, mockProperties1, null);

        ConnectionDao dao2 = new ConnectionDao("test2");
        assertSame(connection2, dao2.getConnection());
    }

    @Test
    public void setAdapterFactory() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        ConnectionDao dao = new ConnectionDao("test");

        assertTrue(dao.createInputAdapter("blah") instanceof SqlInputAdapter);
        assertTrue(dao.createOutputAdapter("blah") instanceof SqlOutAdapter);

        dao.setAdapterFactory(adapterFactory);

        assertSame(inAdapter, dao.createInputAdapter("input"));
        assertSame(outAdapter, dao.createOutputAdapter("output"));
    }
}
package org.sv.flexobject.sql.dao;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.adapter.AdapterFactory;
import org.sv.flexobject.properties.TestBatchEnvironment;
import org.sv.flexobject.sql.SqlInputAdapter;
import org.sv.flexobject.sql.SqlOutAdapter;

import javax.sql.DataSource;

import java.sql.Connection;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceDaoTest {

    @Mock
    DataSource mockDataSource;

    @Mock
    DataSource mockDataSource2;

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

     @Before
    public void setUp() throws Exception {
        TestBatchEnvironment.setDataSource("test", mockDataSource);
        TestBatchEnvironment.setDataSource("test2", mockDataSource2);
        Mockito.when(mockDataSource.getConnection()).thenReturn(connection);
        Mockito.when(mockDataSource2.getConnection()).thenReturn(connection2);
        Mockito.when(adapterFactory.createInputAdapter("input")).thenReturn(inAdapter);
        Mockito.when(adapterFactory.createOutputAdapter("output")).thenReturn(outAdapter);
    }

    @After
    public void tearDown() throws Exception {
        TestBatchEnvironment.clearDataSources();
    }

    @Test
    public void connectionLifecycle() throws Exception {
        DataSourceDao dao = new DataSourceDao("test");
        assertSame(connection, dao.getConnection());
        dao.getConnection();

        dao.close();
        Mockito.verify(connection).close();

        dao.getConnection();
        Mockito.verify(mockDataSource, Mockito.times(2)).getConnection();

        DataSourceDao dao2 = new DataSourceDao("test2");
        assertSame(connection2, dao2.getConnection());
    }

    @Test
    public void setAdapterFactory() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        DataSourceDao dao = new DataSourceDao("test");

        assertTrue(dao.createInputAdapter("blah") instanceof SqlInputAdapter);
        assertTrue(dao.createOutputAdapter("blah") instanceof SqlOutAdapter);

        dao.setAdapterFactory(adapterFactory);

        assertSame(inAdapter, dao.createInputAdapter("input"));
        assertSame(outAdapter, dao.createOutputAdapter("output"));
     }


}
package org.sv.flexobject.sql.dao;

import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class BasicBatchEnvironmentTest {

    Connection connection;

    @After
    public void tearDown() throws Exception {
        if(connection != null){
            connection.close();
            connection = null;
        }
    }

    @Test
    public void createDataSource() throws SQLException {
        DataSource ds = BatchEnvironment.getInstance().loadDataSource("testDB");
        assertNotNull(ds);
        connection = ds.getConnection();
        assertNotNull(connection);
    }

    @Test
    public void getBaseDirs() {
        assertEquals(Arrays.asList(new File("src/test/resources")), BatchEnvironment.getInstance().getBaseDirs());
    }
}
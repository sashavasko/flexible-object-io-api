package org.sv.flexobject.sql.providers;

import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BasicDataSourceProviderTest {

    @Test
    public void getConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "org.h2.Driver");
        properties.setProperty("url", "jdbc:h2:mem:myDb");
        try (BasicDataSourceProvider provider = new BasicDataSourceProvider();
             Connection connection = (Connection) provider.getConnection("foo", properties, null)) {
            assertNotNull(connection);
        }
    }

    @Test
    public void getConnectionWithAuth() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "org.h2.Driver");
        properties.setProperty("url", "jdbc:h2:mem:myDb;USER=sa;PASSWORD=123");
        properties.setProperty("username", "sa");
        try(BasicDataSourceProvider provider = new BasicDataSourceProvider();
            Connection connection = (Connection) provider.getConnection("bar", properties, "123")){
            assertNotNull(connection);
            try{
                properties.setProperty("url", "jdbc:h2:mem:myDb");
                provider.getConnection("badPassword", properties, "123666");
                throw new RuntimeException("Should have thrown due to bad password");
            }catch (Exception e){
                assertEquals(SQLException.class, e.getClass());
                assertEquals("Cannot create PoolableConnectionFactory (Wrong user name or password [28000-200])", e.getMessage());
            }
        }
    }

    @Test
    public void listConnectionTypes() {
        assertEquals(Arrays.asList(Connection.class, PooledSqlConnection.class),new BasicDataSourceProvider().listConnectionTypes());
    }
}
package org.sv.flexobject.sql.providers;

import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class UnPooledConnectionProviderTest {

    @Test
    public void getConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("driverClassName", "org.h2.Driver");
        properties.setProperty("url", "jdbc:h2:mem:myDb");
        UnPooledConnectionProvider provider = new UnPooledConnectionProvider();

        try(Connection connection = (Connection) provider.getConnection("foo", properties, null)) {
            assertNotNull(connection);
        }
    }
}
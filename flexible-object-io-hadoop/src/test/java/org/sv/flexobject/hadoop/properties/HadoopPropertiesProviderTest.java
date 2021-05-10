package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.Test;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.sql.providers.UnPooledConnectionProvider;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;

import static org.junit.Assert.assertNotNull;

public class HadoopPropertiesProviderTest extends ClusterMapReduceTestCase {

    @Test
    public void fullCycle() throws Exception {
        startCluster(true, null);
        Configuration conf = createJobConf();
        FileSystem fs = getFileSystem();
        HadoopSecretProviderTest.publishTestSecret(conf, fs);

        Path expectedConnectionsPath = new Path("/user/" + HadoopTask.getUserName(conf) + "/connections");
        Path expectedPropertiesPath = new Path(expectedConnectionsPath, "alpha/test-connection.xml");

        OutputStream os = fs.create(expectedPropertiesPath);
        try(Writer w = new OutputStreamWriter(os)) {
            w.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><configuration>\n" +
                    "    <property><name>driverClassName</name><value>org.h2.Driver</value></property>\n" +
                    "    <property><name>url</name><value>jdbc:h2:mem:myDb</value></property>\n" +
                    "</configuration>");
        }

        ConnectionManager.addProviders(HadoopSecretProvider.class, HadoopPropertiesProvider.class, UnPooledConnectionProvider.class);
        ConnectionManager.forEachProvider(Configurable.class, (p)->((Configurable)p).setConf(conf));

        try(Connection connection = (Connection) ConnectionManager.getConnection(Connection.class, "test-connection")){
            assertNotNull(connection);
        }
    }
}
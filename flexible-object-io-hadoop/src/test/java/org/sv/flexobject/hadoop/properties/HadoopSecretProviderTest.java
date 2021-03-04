package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.connections.ConnectionManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class HadoopSecretProviderTest extends ClusterMapReduceTestCase {

    Properties testClusterProperties;

    @Before
    public void setUp() throws Exception {
        testClusterProperties = null;//new Properties();
    }

    /*
    * For this test to work - add test-connection password to your jceks on hadoop like so :
    * hdpcreds create org.sv.flexobject.hadoop.db.test-connection.password -value testPassword -provider jceks://hdfs@<adminnode.fqdn>:8022/user/<username>/creds/alpha/secret.jceks
    */

    public static void publishTestSecret(Configuration conf, FileSystem fs) throws IOException {
        Path expectedSecretPath = new Path("/user/" + HadoopSecretProvider.getUserName(conf) + "/creds/alpha/secret.jceks");
        try(OutputStream os = fs.create(expectedSecretPath);
            InputStream is = conf.getClass().getClassLoader().getResourceAsStream("secret.jceks")) {
            byte[] buf = new byte[8192];
            int length;
            while ((length = is.read(buf)) > 0) {
                os.write(buf, 0, length);
            }
        }
    }

    @Test
    public void getSecretFromTestCluster() throws Exception {
        startCluster(true, testClusterProperties);

        HadoopSecretProvider provider = new HadoopSecretProvider();

        Configuration conf = createJobConf();
        provider.setConf(conf);
        publishTestSecret(conf, getFileSystem());

        Object secret = provider.getSecret("test-connection", ConnectionManager.DeploymentLevel.alpha, null);

        assertEquals("testPassword", secret.toString());
    }
}
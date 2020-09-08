package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HadoopBatchEnvironmentTest {

    @Test
    public void loadProperties() {
        Configuration conf = new Configuration();

        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_USERNAME, "GOD");
        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_PASSWORD, "ISGOOD");
        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_DRIVER_CLASS_NAME, "SuperDB");
        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_URL, "jdbc://SuperDB");

        HadoopBatchEnvironment.setConfiguration(conf);
        Properties properties = new HadoopBatchEnvironment().loadProperties("test", "");

        assertEquals("GOD", properties.getProperty(HadoopBatchEnvironment.PROP_USERNAME));
        assertEquals("ISGOOD", properties.getProperty(HadoopBatchEnvironment.PROP_PASSWORD));
        assertEquals("SuperDB", properties.getProperty(HadoopBatchEnvironment.PROP_DRIVER_CLASS_NAME));
        assertEquals("jdbc://SuperDB", properties.getProperty(HadoopBatchEnvironment.PROP_URL));
    }

    @Test
    public void loadPropertiesLdap() {
        Configuration conf = new Configuration();

        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_LDAP, "ldap url");
        conf.set(HadoopBatchEnvironment.HADOOP_DB_NAMESPACE + "test." + HadoopBatchEnvironment.PROP_URL, "jdbc://SuperDB");

        HadoopBatchEnvironment.setConfiguration(conf);
        Properties properties = new HadoopBatchEnvironment().loadProperties("test", "");

        assertEquals("ldap url", properties.getProperty(HadoopBatchEnvironment.PROP_LDAP));
        assertEquals("true", properties.getProperty(HadoopBatchEnvironment.PROP_DATABASE_NAME_IN_LDAP));
        assertNull( properties.getProperty(HadoopBatchEnvironment.PROP_URL));

    }

    @Test
    public void getAllDirsReturnsNull() {
        assertNull(new HadoopBatchEnvironment().getBaseDirs());
    }
}
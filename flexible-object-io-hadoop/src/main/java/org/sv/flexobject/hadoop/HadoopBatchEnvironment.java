package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.sql.dao.BasicBatchEnvironment;

import java.io.File;
import java.util.List;
import java.util.Properties;

public class HadoopBatchEnvironment extends BasicBatchEnvironment {

    public static final String DEFAULT_NAMESPACE = "org.sv.flexobject.hadoop";

    //TODO should probably rewrite it using HadoopPropertiesWrapper

    public static final String HADOOP_DB_NAMESPACE = DEFAULT_NAMESPACE + ".db.";
    public static final String PROP_USERNAME = "username";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_DRIVER_CLASS_NAME = "driverClassName";
    public static final String PROP_URL = "url";
    public static final String PROP_LDAP = "ldap";
    public static final String PROP_DATABASE_NAME_IN_LDAP = "databaseNameInLdap";
    static Configuration configuration = null;

    public static void setConfiguration(Configuration configuration) {
        HadoopBatchEnvironment.configuration = configuration;
    }

    protected void setProperty(Properties properties, String appName, String propertyName){
        String value = configuration.get(HADOOP_DB_NAMESPACE + appName + "." + propertyName);
        if (StringUtils.isNotBlank(value))
            properties.setProperty(propertyName, value);
    }

    @Override
    public Properties loadProperties(String appName, String propertiesSubdirectory) {
        Properties properties = new Properties();

        String ldap = configuration.get(HADOOP_DB_NAMESPACE + appName + "." + PROP_LDAP);
        if (StringUtils.isNotBlank(ldap)) {
            properties.setProperty(PROP_LDAP, ldap);
            properties.setProperty(PROP_DATABASE_NAME_IN_LDAP, "true");
        }else
            setProperty(properties, appName, PROP_URL);
        setProperty(properties, appName, PROP_USERNAME);
        setProperty(properties, appName, PROP_PASSWORD);
        setProperty(properties, appName, PROP_DRIVER_CLASS_NAME);

        return properties;
    }

    @Override
    protected List<File> getBaseDirs() {
        return null;
    }
}

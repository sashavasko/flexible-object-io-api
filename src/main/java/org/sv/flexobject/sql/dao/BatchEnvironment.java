package org.sv.flexobject.sql.dao;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

public abstract class BatchEnvironment {
    private static final String DATABASE_SUBDIRECTORY = "db";

    public Properties loadProperties(String appName, String propertiesSubdirectory){
        Properties properties = new Properties();
        boolean propertiesLoaded = false;
        for (File baseDirectory : getBaseDirs()) {
            File propertiesDirectory = new File(baseDirectory, propertiesSubdirectory);
            logProgress("Loading properties from " + propertiesDirectory.getAbsolutePath());
            File file = new File(propertiesDirectory, appName + ".properties");
            if (file.exists()) {
                try {
                    FileInputStream inputStream = createFileInputStream(file);
                    properties.load(inputStream);
                    inputStream.close();
                    propertiesLoaded = true;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!propertiesLoaded) {
            throw new RuntimeException("Could not find properties file in "  + getBaseDirs() + " ("+ appName + ".properties)");
        }
        return properties;
    }

    public DataSource loadDataSource(String connectionName) {
        Properties props = loadProperties(connectionName, DATABASE_SUBDIRECTORY);

        return createDataSource(props);
    }

    public static BatchEnvironment getInstance() {
        try {
            return (BatchEnvironment) BatchEnvironment.class.getClassLoader().loadClass("org.sv.flexobject.properties.TestBatchEnvironment")
                    .newInstance();
        } catch (Exception e) {
        }
        try {
            return (BatchEnvironment) BatchEnvironment.class.getClassLoader().loadClass("org.sv.flexobject.properties.MainBatchEnvironment").newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to find default batch environment", e);
        }
    }

    protected FileInputStream createFileInputStream(File file) throws FileNotFoundException {
        return new FileInputStream(file);
    }

    protected abstract void logProgress(String message);

    protected abstract DataSource createDataSource(Properties props);

    protected abstract List<File> getBaseDirs();

}

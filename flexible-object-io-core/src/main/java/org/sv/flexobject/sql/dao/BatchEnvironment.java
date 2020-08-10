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
    public static final String TEST_BATCH_ENVIRONMENT_CLASS = "org.sv.flexobject.properties.TestBatchEnvironment";
    public static final String MAIN_BATCH_ENVIRONMENT_CLASS = "org.sv.flexobject.properties.MainBatchEnvironment";

    public static BatchEnvironment getInstance() {
        try {
            return (BatchEnvironment) BatchEnvironment.class.getClassLoader().loadClass(TEST_BATCH_ENVIRONMENT_CLASS)
                    .newInstance();
        } catch (Exception e) {
        }
        try {
            return (BatchEnvironment) BatchEnvironment.class.getClassLoader().loadClass(MAIN_BATCH_ENVIRONMENT_CLASS)
                    .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to find default batch environment. Make sure your project has " +
                    MAIN_BATCH_ENVIRONMENT_CLASS + " class extending BatchEnvironment.", e);
        }
    }

    public Properties loadProperties(String appName, String propertiesSubdirectory){
        Properties properties = new Properties();
        List<File> baseDirs = getBaseDirs();
        if (baseDirs != null && !baseDirs.isEmpty()) {
            loadPropertiesFromFiles(appName, propertiesSubdirectory, baseDirs, properties);
            if (properties.isEmpty()) {
                throw new RuntimeException("Could not find properties file in " + getBaseDirs() + " (" + appName + ".properties)");
            }
        }
        loadOtherProperties(appName, properties);
        return properties;
    }

    public DataSource loadDataSource(String connectionName) {
        Properties props = loadProperties(connectionName, DATABASE_SUBDIRECTORY);

        return createDataSource(props);
    }

     protected void loadPropertiesFromFiles(String appName, String propertiesSubdirectory, List<File> baseDirs, Properties properties){
        for (File baseDirectory : baseDirs) {
            File propertiesDirectory = new File(baseDirectory, propertiesSubdirectory);
            logProgress("Loading properties from " + propertiesDirectory.getAbsolutePath());
            File file = new File(propertiesDirectory, appName + ".properties");
            if (file.exists()) {
                try(FileInputStream inputStream = createFileInputStream(file)) {
                    properties.load(inputStream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    protected void loadOtherProperties(String appName, Properties properties) {

    }

    protected FileInputStream createFileInputStream(File file) throws FileNotFoundException {
        return new FileInputStream(file);
    }

    protected List<File> getBaseDirs(){return null;};

    protected abstract void logProgress(String message);

    protected abstract DataSource createDataSource(Properties props);


}

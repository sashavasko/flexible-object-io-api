package org.sv.flexobject.properties;

import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

public class FilePropertiesProvider implements PropertiesProvider {

    public static final String DEFAULT_EXTENSION = ".properties";
    private String fileExtension = DEFAULT_EXTENSION;

    private List<String> pathsToFiles;

    public FilePropertiesProvider(List<String> pathsToFiles) {
        this.pathsToFiles = pathsToFiles;
    }

    public FilePropertiesProvider(String ... pathsToFiles) {
        this.pathsToFiles = Arrays.asList(pathsToFiles);
    }

    public FilePropertiesProvider() {
    }

    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    public void setPathsToFiles(List<String> pathsToFiles) {
        this.pathsToFiles = pathsToFiles;
    }

    public void setPathsToFiles(String ... pathsToFiles) {
        this.pathsToFiles = Arrays.asList(pathsToFiles);
    }

    protected Properties load(InputStream is) throws IOException {
        Properties allProperties = new Properties();
        allProperties.load(is);
        return allProperties;
    }

    Properties filter (String connectionName, Stack<String> hierarchy, InputStream inputStream, boolean connectionSpecific) throws IOException {
        Properties properties = load(inputStream);
        Properties connectionProperties = filter(Namespace.forPath(hierarchy, connectionName), properties);
        if (connectionProperties.isEmpty() && connectionSpecific)
            connectionProperties = filter(Namespace.forPath(hierarchy), properties);
        return connectionProperties;
    }

    protected PropertiesFile makeFile(String path, String name) throws IOException{
        return new PropertiesFile(path, name);
    }


    protected Properties findFile(String connectionName, String path, Stack<String> hierarchy) throws Exception {
        PropertiesFile file = makeFile(path, connectionName + fileExtension);
        if (file.exists())
            return filter(connectionName, hierarchy, file.open(), true);
        else if (hierarchy != null && !hierarchy.isEmpty()) {
            String top = hierarchy.pop();

            file = makeFile(path, top + fileExtension);
            if (file.exists()) {
                return filter(connectionName, hierarchy, file.open(), false);
            }
            file = makeFile(path, top);
            if (file.exists() && file.isDirectory()) {
                return findFile(connectionName, file.getPath(), hierarchy);
            }
        }
        return null;
    }

    private Stack<String> makeHierarchy(Stack<String> stack, String ... items){
        while (!stack.empty()) stack.pop();
        for (String item : items)
            stack.push(item);
        return stack;
    }

    protected Properties findFile(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) throws Exception {
        Properties connectionProperties = null;
        Stack<String> hierarchy = new Stack<>();
        for (String path : pathsToFiles){
            connectionProperties = findFile(connectionName, path, makeHierarchy(hierarchy, deploymentLevel.name(), environment));
            if (connectionProperties != null && !connectionProperties.isEmpty())
                return connectionProperties;

            connectionProperties = findFile(connectionName, path, makeHierarchy(hierarchy, environment, deploymentLevel.name()));
            if (connectionProperties != null && !connectionProperties.isEmpty())
                return connectionProperties;

            connectionProperties = findFile(connectionName, path, null);
            if (connectionProperties != null && !connectionProperties.isEmpty())
                return connectionProperties;
        }
        return null;
    }


    @Override
    public Properties getProperties(String connectionName, ConnectionManager.DeploymentLevel deploymentLevel, String environment) {
        try {
            return findFile(connectionName, deploymentLevel, environment);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}

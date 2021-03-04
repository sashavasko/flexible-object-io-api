package org.sv.flexobject.properties;

import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Stack;

public class FilePropertiesProvider implements PropertiesProvider {

    public static final String DEFAULT_FILE_EXTENSION = ".properties";

    private List<String> pathsToFiles;

    public FilePropertiesProvider(List<String> pathsToFiles) {
        setPathsToFiles(pathsToFiles);
    }

    public FilePropertiesProvider(String ... pathsToFiles) {
        setPathsToFiles(pathsToFiles);
    }

    public FilePropertiesProvider() {
    }

    public String getFileExtension() {
        return DEFAULT_FILE_EXTENSION;
    }

    public void setPathsToFiles(List<String> pathsToFiles) {
        this.pathsToFiles = pathsToFiles;
    }

    public void setPathsToFiles(String ... pathsToFiles) {
        setPathsToFiles(Arrays.asList(pathsToFiles));
    }

    public List<String> getPathsToFiles() {
        return pathsToFiles;
    }

    protected Properties load(byte[] data) throws IOException {
        Properties allProperties = new Properties();

        int firstNonSpace = 0;
        for(; firstNonSpace < data.length && Character.isWhitespace(data[firstNonSpace]); firstNonSpace++);

        switch(data[firstNonSpace]){
            case '<' : return loadXml(data, allProperties);
            case '{' :
            case '[' : return loadJson(data, allProperties);
            default :
                allProperties.load(new ByteArrayInputStream(data));
                return allProperties;
        }
    }

    protected Properties loadJson(byte[] data, Properties allProperties) throws IOException {
        // TODO
        //JsonNode json = MapperFactory.getObjectReader().readTree(new ByteArrayInputStream(data));
        // return allProperties;
        throw new UnsupportedOperationException("JSON parsing into Properties is not implemented");
    }

    protected Properties loadXml(byte[] data, Properties allProperties) {
        throw new UnsupportedOperationException("XML parsing into Properties is not implemented");
    }

    Properties filter (String connectionName, Stack<String> hierarchy, byte[] data, boolean connectionSpecific) throws IOException {
        Properties properties = load(data);
        Properties connectionProperties = filter(Namespace.forPath(hierarchy, connectionName), properties);
        if (connectionProperties.isEmpty() && connectionSpecific)
            connectionProperties = filter(Namespace.forPath(hierarchy), properties);
        return connectionProperties;
    }

    protected PropertiesFile makeFile(String path, String name) throws IOException{
        return new PropertiesFile(path, name);
    }


    protected Properties findFile(String connectionName, String path, Stack<String> hierarchy) throws Exception {
        PropertiesFile file = makeFile(path, connectionName + getFileExtension());
        if (file.exists())
            return filter(connectionName, hierarchy, file.readFully(), true);
        else if (hierarchy != null && !hierarchy.isEmpty()) {
            String top = hierarchy.pop();

            file = makeFile(path, top + getFileExtension());
            if (file.exists()) {
                return filter(connectionName, hierarchy, file.readFully(), false);
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
        for (String path : getPathsToFiles()){
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

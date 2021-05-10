package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.utils.IConfigured;
import org.sv.flexobject.properties.FilePropertiesProvider;
import org.sv.flexobject.properties.PropertiesFile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class HadoopPropertiesProvider extends FilePropertiesProvider implements IConfigured {
    @Override
    public String getFileExtension() {
        return ".xml";
    }

    @Override
    protected Properties loadXml(byte[] data, Properties allProperties) {
        Configuration conf = new Configuration(false);
        conf.addResource(new ByteArrayInputStream(data));
        conf.reloadConfiguration();
        conf.forEach((e) -> allProperties.setProperty(e.getKey(), e.getValue()));
        return allProperties;
    }

    public List<String> getHadoopPathToFiles(){
        return Arrays.asList( "/user/" + HadoopTask.getUserName(getConf()) + "/connections");
    }

    @Override
    public List<String> getPathsToFiles() {
        List<String> setPathToFiles = super.getPathsToFiles();
        return setPathToFiles == null ? getHadoopPathToFiles() : setPathToFiles;
    }

    @Override
    protected PropertiesFile makeFile(String path, String name) throws IOException {
        return new HadoopPropertiesFile(path, name, getConf());
    }
}

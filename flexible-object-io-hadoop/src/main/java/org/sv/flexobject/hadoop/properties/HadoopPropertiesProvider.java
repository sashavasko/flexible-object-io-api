package org.sv.flexobject.hadoop.properties;

import org.sv.flexobject.hadoop.utils.IConfigured;
import org.sv.flexobject.properties.FilePropertiesProvider;
import org.sv.flexobject.properties.PropertiesFile;

import java.io.IOException;

public class HadoopPropertiesProvider extends FilePropertiesProvider implements IConfigured {

    @Override
    protected PropertiesFile makeFile(String path, String name) throws IOException {
        return new HadoopPropertiesFile(path, name, getConf());
    }

}

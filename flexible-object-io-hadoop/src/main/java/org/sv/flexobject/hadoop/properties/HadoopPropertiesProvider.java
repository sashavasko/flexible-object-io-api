package org.sv.flexobject.hadoop.properties;

import com.carfax.dt.streaming.properties.FilePropertiesProvider;
import com.carfax.dt.streaming.properties.PropertiesFile;
import com.carfax.hadoop.utils.IConfigured;

import java.io.IOException;

public class HadoopPropertiesProvider extends FilePropertiesProvider implements IConfigured {

    @Override
    protected PropertiesFile makeFile(String path, String name) throws IOException {
        return new HadoopPropertiesFile(path, name, getConf());
    }

}

package org.sv.flexobject.mongo.streaming;

import org.sv.flexobject.util.InstanceFactory;

import java.util.Map;

public class MongoMapSourceBuilder extends MongoBuilder<MongoMapSourceBuilder, MongoMapSource>{

    @Override
    public MongoMapSource build() throws Exception {
        MongoMapSource source = InstanceFactory.get(MongoMapSource.class);
        source.setCursor(getCursor(Map.class));
        saveConnection(source);
        return source;
    }

    public static MongoMapSourceBuilder newInstance() {
        return InstanceFactory.get(MongoMapSourceBuilder.class);
    }
}

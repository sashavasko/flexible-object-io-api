package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.sv.flexobject.util.InstanceFactory;

public class MongoPOJOSource<POJO> extends MongoCursorSource<POJO,POJO> {
    public MongoPOJOSource() {
    }

    public MongoPOJOSource(MongoCursor<POJO> cursor) {
        super(cursor);
    }

    public static class Builder extends MongoBuilder<Builder, MongoPOJOSource>{

        @Override
        public MongoPOJOSource build() throws Exception {
            MongoPOJOSource source = InstanceFactory.get(MongoPOJOSource.class);
            source.setCursor(getCursor(getDocumentClass()));
            saveConnection(source);
            return source;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
    }

    @Override
    public POJO get() throws Exception {
        return getCursor().next();
    }
}

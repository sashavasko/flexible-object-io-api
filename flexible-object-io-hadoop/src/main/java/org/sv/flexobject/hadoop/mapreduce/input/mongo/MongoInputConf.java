package org.sv.flexobject.hadoop.mapreduce.input.mongo;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.mongo.connection.MongoConnection;

public class MongoInputConf<SELF extends HadoopPropertiesWrapper> extends HadoopPropertiesWrapper<SELF> {
    public static final String SUBNAMESPACE = "input.mongo";

    protected String connectionName;
    protected String dbName;
    protected String collectionName;

    protected Class<? extends StreamableWithSchema> schema;

    public MongoInputConf() {
        super();
    }

    @Override
    public SELF setDefaults() {
        return (SELF)this;
    }

    public MongoInputConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public Class<? extends StreamableWithSchema> getInputSchema() {
        return schema;
    }

    public boolean hasSchema() {
        return schema != null;
    }

    public MongoConnection getMongo() throws Exception {
        return MongoConnection.builder().forName(getConnectionName()).db(getDbName()).build();
    }

}

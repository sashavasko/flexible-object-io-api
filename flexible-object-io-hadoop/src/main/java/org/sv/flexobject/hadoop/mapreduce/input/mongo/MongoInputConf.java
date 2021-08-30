package org.sv.flexobject.hadoop.mapreduce.input.mongo;


import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.mongo.streaming.MongoBuilder;
import org.sv.flexobject.mongo.streaming.MongoDocumentSource;
import org.sv.flexobject.util.InstanceFactory;

public class MongoInputConf<SELF extends HadoopPropertiesWrapper> extends HadoopPropertiesWrapper<SELF> {
    public static final String SUBNAMESPACE = "input.mongo";

    protected String connectionName;
    protected String dbName;
    protected String collectionName;
    protected int estimateSizeLimit;
    protected int estimateTimeLimitMicros;
    protected Class<? extends MongoBuilder> sourceBuilderClass;

    protected Class<? extends StreamableWithSchema> schema;

    public MongoInputConf() {
        super();
    }

    @Override
    public SELF setDefaults() {
        estimateSizeLimit = 100000;
        estimateTimeLimitMicros = 1000;
        sourceBuilderClass = MongoDocumentSource.Builder.class;
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

    public int getEstimateSizeLimit() {
        return estimateSizeLimit;
    }

    public int getEstimateTimeLimitMicros() {
        return estimateTimeLimitMicros;
    }

    public MongoBuilder getSourceBuilder() {
        MongoBuilder builder = InstanceFactory.get(sourceBuilderClass);
        builder.connection(getConnectionName())
                .db(getDbName())
                .collection(getCollectionName());

        if (hasSchema())
            builder.schema(getInputSchema());

        return builder;
    }

}

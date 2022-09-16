package org.sv.flexobject.hadoop.mapreduce.input.mongo;


import com.mongodb.client.MongoCollection;
import org.apache.log4j.Logger;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.mongo.streaming.MongoBuilder;
import org.sv.flexobject.mongo.streaming.MongoDocumentSource;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

public class MongoInputConf<SELF extends HadoopPropertiesWrapper> extends InputConf<SELF> {
    public static final Logger logger = Logger.getLogger(MongoInputConf.class);

    public static final String SUBNAMESPACE = "mongo";

    protected String connectionName;
    protected String dbName;
    protected String collectionName;
    protected int estimateSizeLimit;
    protected int estimateTimeLimitMicros;
    protected Class<? extends MongoBuilder> builderClass;

    protected Class<? extends Streamable> schema;

    public MongoInputConf() {
        super(SUBNAMESPACE);
    }

    public MongoInputConf(String child) {
        super(makeMyNamespace(getParentNamespace(MongoInputConf.class), SUBNAMESPACE), child);
    }

    public MongoInputConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public MongoInputConf(Namespace parent, String child) {
        super(parent, child);
    }

    @Override
    public SELF setDefaults() {
        super.setDefaults();
        estimateSizeLimit = 100000;
        estimateTimeLimitMicros = 1000;
        builderClass = MongoDocumentSource.Builder.class;
        sourceBuilderClass = MongoSourceBuilder.class;
        return (SELF)this;
    }

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public void setBuilderClass(Class<? extends MongoBuilder> builderClass) {
        this.builderClass = builderClass;
    }

    public void setSchema(Class<? extends Streamable> schema) {
        this.schema = schema;
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

    public Class<? extends Streamable> getInputSchema() {
        return schema;
    }

    public boolean hasSchema() {
        return schema != null;
    }

    public MongoConnection getMongo() throws Exception {
        logger.info("Making Mongo for connection: " + connectionName + ", db: " + dbName);
        return MongoConnection.builder().forName(getConnectionName()).db(getDbName()).build();
    }

    public int getEstimateSizeLimit() {
        return estimateSizeLimit;
    }

    public int getEstimateTimeLimitMicros() {
        return estimateTimeLimitMicros;
    }

    public MongoBuilder getMongoBuilder() {
        logger.info("Making MongoBuilder for class: " + builderClass + ", connection: " + connectionName + ", db: " + dbName + ", collection:" + collectionName);
        MongoBuilder builder = InstanceFactory.get(builderClass);
        builder.connection(getConnectionName())
                .db(getDbName())
                .collection(getCollectionName());

        if (hasSchema()) {
            builder.schema(getInputSchema());
            logger.info("Input schema set to: " + getInputSchema());
        }
        return builder;
    }

    public MongoCollection getCollection() throws Exception {
        logger.info("Making MongoBuilder for class: " + builderClass + ", connection: " + connectionName + ", db: " + dbName + ", collection:" + collectionName);
        MongoBuilder builder = InstanceFactory.get(builderClass);
        builder.connection(getConnectionName())
                .db(getDbName())
                .collection(getCollectionName());
        return builder.getCollection();
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }
}

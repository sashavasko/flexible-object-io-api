package org.sv.flexobject.mongo.streaming;

import com.mongodb.CursorType;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.mongo.MongoClientProvider;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.stream.Source;

public abstract class MongoBuilder<SELF extends MongoBuilder, SOURCE extends Source> implements AutoCloseable{
    protected String connectionName;
    protected String dbName;
    private MongoConnection connection;
    protected String hosts;
    private boolean ownConnection = true;
    protected String collectionName;
    protected Bson filter;
    protected Bson projection;
    protected Bson sort;
    protected Integer limit;
    protected Integer skip;
    protected Boolean notimeout = false;
    protected CursorType cursorType;
    private MongoCursor cursor;
    private Class documentClass = Document.class;
    private Class<? extends Streamable> schema;

    public SELF connection(String connectionName) {
        this.connectionName = connectionName;
        return (SELF) this;
    }

    public SELF connection(MongoConnection connection) {
        this.connection = connection;
        ownConnection = false;
        return (SELF) this;
    }

    public SELF hosts(String hosts) {
        this.hosts = hosts;
        return (SELF) this;
    }

    public SELF db(String dbName) {
        this.dbName = dbName;
        return (SELF) this;
    }

    public SELF collection(String collectionName) {
        this.collectionName = collectionName;
        return (SELF) this;
    }

    public SELF filter(Bson query) {
        this.filter = query;
        return (SELF) this;
    }

    public SELF projection(Bson projection) {
        this.projection = projection;
        return (SELF) this;
    }

    public SELF sort(Bson sort) {
        this.sort = sort;
        return (SELF) this;
    }

    public SELF limit(Integer limit) {
        this.limit = limit;
        return (SELF) this;
    }

    public SELF skip(Integer skip) {
        this.skip = skip;
        return (SELF) this;
    }

    public SELF noTimeout() {
        this.notimeout = true;
        return (SELF) this;
    }

    public SELF cursorType(CursorType cursorType) {
        this.cursorType = cursorType;
        return (SELF) this;
    }

    public SELF schema(Class<? extends Streamable> schema) {
        this.schema = schema;
        return (SELF) this;
    }

    public SELF forDocumentClass(Class outClass) {
        this.documentClass = outClass;
        return (SELF) this;
    }

    public SELF forCursor(FindIterable iterable){
        this.cursor = iterable.cursor();
        return (SELF) this;
    }

    public MongoConnection getConnection() throws Exception {
        if (connection == null){
            MongoConnection.Builder builder = MongoConnection.builder()
                    .forName(connectionName)
                    .db(dbName);
            if (StringUtils.isNotBlank(hosts))
                builder.override("hosts", hosts);

            connection = builder.build();
            ownConnection = true;
        } else {
            ownConnection = false;
        }
        return connection;
    }

    public MongoCollection getCollection() throws Exception {
        return getConnection().getCollection(collectionName, documentClass);
    }

    public <TDocument> MongoCursor<TDocument> getCursor(Class<TDocument> documentClass) throws Exception {
        if (cursor == null){
            forDocumentClass(documentClass);
            MongoCollection<TDocument> collection = getCollection();
            FindIterable<TDocument> findIterable = collection.find();
            if (filter != null) {
                findIterable = findIterable.filter(filter);
                System.out.println("Using filter: " + filter.toBsonDocument().toJson());
            }else
                System.out.println("No filter specified");
            if (limit != null)
                findIterable = findIterable.limit(limit);
            if (skip != null)
                findIterable = findIterable.skip(skip);
            if (projection != null)
                findIterable = findIterable.projection(projection);
            if (sort != null) {
                findIterable = findIterable.sort(sort);
                System.out.println("Using sort :" + sort.toBsonDocument().toJson());
            }
            if (notimeout != null)
                findIterable = findIterable.noCursorTimeout(true);

            if (cursorType != null)
                findIterable = findIterable.cursorType(CursorType.TailableAwait);
            cursor = findIterable.cursor();
        }
        return cursor;
    }

    public Class<? extends Streamable> getSchema() throws SchemaException {
        if (schema == null)
            return null;
        if (!Streamable.class.isAssignableFrom(schema))
            throw new SchemaException("Schema is not available for classes not derived from Streamable. Requested class is " + schema.getName());
        return schema;
    }

    public BsonSchema getBsonSchema() throws SchemaException {
        return getSchema() != null ?
                BsonSchema.getRegisteredSchema(getSchema()) : null;
    }

    public Class getDocumentClass() {
        return documentClass;
    }

    public boolean isOwnConnection() {
        return ownConnection;
    }

    public void saveConnection(MongoConnectionOwner owner) throws Exception {
        if (isOwnConnection())
            owner.setConnection(getConnection());
    }

    abstract public SOURCE build() throws Exception;

    @Override
    public void close() throws Exception {
        if (connection != null && isOwnConnection()){
            connection.close();
            connection = null;
        }
    }
}

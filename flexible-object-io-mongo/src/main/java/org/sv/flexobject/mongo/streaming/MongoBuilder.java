package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.conversions.Bson;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.mongo.schema.BsonSchema;

public class MongoBuilder<SELF extends MongoBuilder> {
    protected String connectionName;
    protected String dbName;
    private MongoConnection connection;
    private boolean ownConnection = true;
    protected String collectionName;
    protected Bson filter;
    protected Bson projection;
    protected Bson sort;
    protected Integer limit;
    protected Integer skip;
    protected Boolean notimeout = false;
    private MongoCursor cursor;
    private Class documentClass;

    public SELF connection(String connectionName) {
        this.connectionName = connectionName;
        return (SELF) this;
    }

    public SELF connection(MongoConnection connection) {
        this.connection = connection;
        ownConnection = false;
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

    public SELF schema(Class<? extends StreamableWithSchema> schema) {
        this.documentClass = schema;
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
            connection = MongoConnection.builder()
                    .forName(connectionName)
                    .db(dbName).build();
            ownConnection = true;
        } else {
            ownConnection = false;
        }
        return connection;
    }

    public <TDocument> MongoCursor<TDocument> getCursor(Class<TDocument> documentClass) throws Exception {
        if (cursor == null){
            MongoCollection<TDocument> collection = getConnection().getCollection(collectionName, documentClass);
            FindIterable<TDocument> findIterable = collection.find();
            if (filter != null)
                findIterable = findIterable.filter(filter);
            if (limit != null)
                findIterable = findIterable.limit(limit);
            if (skip != null)
                findIterable = findIterable.skip(skip);
            if (projection != null)
                findIterable = findIterable.projection(projection);
            if (sort != null)
                findIterable = findIterable.sort(sort);
            if (notimeout != null)
                findIterable = findIterable.noCursorTimeout(true);

            cursor = findIterable.cursor();
        }
        return cursor;
    }

    public Class<? extends StreamableWithSchema> getSchema() {
        return StreamableWithSchema.class.isAssignableFrom(documentClass) ? documentClass : null;
    }

    public BsonSchema getBsonSchema() {
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
}
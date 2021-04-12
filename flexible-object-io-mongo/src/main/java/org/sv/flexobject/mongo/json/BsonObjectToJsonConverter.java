package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.DBObject;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

public class BsonObjectToJsonConverter {

    private static BsonObjectToJsonConverter instance;

    public JsonNodeWriter writer = new JsonNodeWriter(JsonWriterSettings.builder().build());

    private BsonObjectToJsonConverter() {
    }

    public static BsonObjectToJsonConverter getInstance() {
        if (instance == null)
            instance = new BsonObjectToJsonConverter();
        return instance;
    }

    public JsonNode convert(Object value) throws IOException {
        writer.reset();
        Encoder encoder;
        if (value instanceof DBObject)
            encoder = getDefaultCodecRegistry().get(DBObject.class);
        else if (value instanceof Document)
            encoder = getDefaultCodecRegistry().get(Document.class);
        else if (value instanceof RawBsonDocument)
            encoder = getDefaultCodecRegistry().get(RawBsonDocument.class);
        else
            throw new IOException("Unknown BSON document type " + value.getClass());

        encoder.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getRoot();
    }
}
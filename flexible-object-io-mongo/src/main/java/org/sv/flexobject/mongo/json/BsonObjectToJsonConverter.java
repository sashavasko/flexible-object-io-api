package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.DBObject;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

public class BsonObjectToJsonConverter {
    public Encoder DBObjectEncoder = getDefaultCodecRegistry().get(DBObject.class);
    public Encoder DocumentEncoder = getDefaultCodecRegistry().get(Document.class);
    public JsonNodeWriter writer = new JsonNodeWriter(JsonWriterSettings.builder().build());

    public JsonNode convert(Object value) throws IOException {
        writer.reset();
        Encoder encoder = DocumentEncoder;
        if (value instanceof DBObject)
            encoder = DBObjectEncoder;
        encoder.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getRoot();
    }
}
package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

public class SimplisticBsonObjectToJsonConverter {

    private static SimplisticBsonObjectToJsonConverter simplisticInstance;
    final private SimplisticJsonNodeWriter writer = new SimplisticJsonNodeWriter(JsonWriterSettings.builder().build());

    private SimplisticBsonObjectToJsonConverter() {
    }

    public static SimplisticBsonObjectToJsonConverter getInstance() {
        if (simplisticInstance == null)
            simplisticInstance = new SimplisticBsonObjectToJsonConverter();
        return simplisticInstance;
    }

    public JsonNode convert(Object value) throws IOException {
        writer.reset();
        Encoder encoder = getDefaultCodecRegistry().get(value.getClass());
        if (encoder == null)
            throw new IOException("Unknown BSON document type " + value.getClass());

        encoder.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return writer.getRoot();
    }
}
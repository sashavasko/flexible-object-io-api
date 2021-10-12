package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.sinks.SingleValueSink;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

public class BsonToJsonConverter {
    public static final Logger logger = LogManager.getLogger(BsonToJsonConverter.class);

    JsonWriterSettings writerSettings;

    JsonNodeWriter singleValueWriter;

    public BsonToJsonConverter() {
        this(JsonWriterSettings.builder().build());
    }

    public BsonToJsonConverter(JsonWriterSettings writerSettings) {
        this.writerSettings = writerSettings;
    }

    public JsonNode convert(Object value) throws IOException {
        if (singleValueWriter == null){
            Sink<JsonNode> singleValueSink = new SingleValueSink<>();
            singleValueWriter = new JsonNodeWriter(singleValueSink, writerSettings);
        }

        Encoder encoder = getDefaultCodecRegistry().get(value.getClass());
        if (encoder == null)
            throw new IOException("Unknown BSON document type " + value.getClass());

        encoder.encode(singleValueWriter, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return singleValueWriter.getSink().get();
    }

    public void convertAll(Source source, Sink<JsonNode> sink, ProgressReporter progressReporter) throws Exception {
        JsonNodeWriter writer = new JsonNodeWriter(sink, writerSettings);
        Object value;
        while ((value = source.get()) != null){
            Encoder encoder = getDefaultCodecRegistry().get(value.getClass());
            if (encoder == null)
                logger.error("Unknown BSON document type " + value.getClass());
            else {
                encoder.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
                progressReporter.increment();
            }
        }
    }


}

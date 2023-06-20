package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoClientSettings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.UuidRepresentation;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.sv.flexobject.mongo.json.converters.HexObjectIdConverter;
import org.sv.flexobject.mongo.json.converters.NormalDateConverter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.sinks.SingleValueSink;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class BsonToJsonConverter {
    public static final Logger logger = LogManager.getLogger(BsonToJsonConverter.class);

    JsonWriterSettings writerSettings;

    JsonNodeWriter singleValueWriter;

    public static BsonToJsonConverter relaxed() {
        return new BsonToJsonConverter(JsonWriterSettings.builder()
                .outputMode(JsonMode.RELAXED)
                .build());
    }

    public static BsonToJsonConverter extended() {
        return new BsonToJsonConverter(JsonWriterSettings.builder()
                .outputMode(JsonMode.EXTENDED)
                .build());
    }

    public static BsonToJsonConverter shell() {
        return new BsonToJsonConverter(JsonWriterSettings.builder()
                .outputMode(JsonMode.SHELL)
                .build());
    }
    public static BsonToJsonConverter relaxedNoDatesAndNoObjectId() {
        return new BsonToJsonConverter(JsonWriterSettings.builder()
                .outputMode(JsonMode.RELAXED)
                .dateTimeConverter(new NormalDateConverter()) // disable $date
                .objectIdConverter(new HexObjectIdConverter())// disable $oid
                .build());
    }

    public BsonToJsonConverter(JsonWriterSettings writerSettings) {
        this.writerSettings = writerSettings;
    }
    private Encoder makeEncoder(Object value){
        CodecRegistry codecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry());
        CodecRegistry uuidRegistry = CodecRegistries.withUuidRepresentation(codecRegistry, UuidRepresentation.STANDARD);
        return uuidRegistry.get(value.getClass());
    }

    public JsonNode convert(Object value) throws IOException {
        if (singleValueWriter == null){
            Sink<JsonNode> singleValueSink = new SingleValueSink<>();
            singleValueWriter = new JsonNodeWriter(singleValueSink, writerSettings);
        }

        Encoder encoder = makeEncoder(value);
        if (encoder == null)
            throw new IOException("Unknown BSON document type " + value.getClass());

        encoder.encode(singleValueWriter, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return singleValueWriter.getSink().get();
    }

    public void convertAll(Source source, Sink<JsonNode> sink, ProgressReporter progressReporter) throws Exception {
        JsonNodeWriter writer = new JsonNodeWriter(sink, writerSettings);
        Object value;
        while ((value = source.get()) != null){
            Encoder encoder = makeEncoder(value);
            if (encoder == null)
                logger.error("Unknown BSON document type " + value.getClass());
            else {
                encoder.encode(writer, value, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
                progressReporter.increment();
            }
        }
    }


}

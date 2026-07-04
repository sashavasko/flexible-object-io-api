package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class SimplisticJsonNodeWriterTest {

    static ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void complexObjectJson() throws Exception {
        TestDataWithInferredSchema testData = TestDataWithInferredSchema.random(true);

        JsonNode json = testData.toJson();
        Document document = Document.parse(json.toString());

        SimplisticJsonNodeWriter writer = new SimplisticJsonNodeWriter(JsonWriterSettings.builder().build());
        Encoder encoder = getDefaultCodecRegistry().get(Document.class);
        encoder.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        JsonNode actualJson = writer.getRoot();

        System.out.println(actualJson);
        assertEquals(json, actualJson);
    }

    public void doTest(String file) throws IOException {
        String simpleReportJson = IOUtils.toString(
                this.getClass().getResourceAsStream(file),
                "UTF-8"
        );
        BasicDBObject dbObject = BasicDBObject.parse(simpleReportJson);

        SimplisticJsonNodeWriter writer = new SimplisticJsonNodeWriter(JsonWriterSettings.builder().build());
        Encoder encoder = getDefaultCodecRegistry().get(BasicDBObject.class);
        encoder.encode(writer,  dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        JsonNode cleanJson = objectMapper.readTree(dbObject.toJson());

        assertEquals (cleanJson.toString(), writer.getRoot().toString());
        assertEquals (cleanJson.toString(), objectMapper.writeValueAsString(writer.getRoot()));
    }

    static JsonWriterSettings jsonWriterSettings = JsonWriterSettings.builder().build();
    static Encoder encoder = getDefaultCodecRegistry().get(BasicDBObject.class);
    static SimplisticJsonNodeWriter writer = new SimplisticJsonNodeWriter(jsonWriterSettings);

    public static String basicDBObjectToJson (BasicDBObject dbObject) throws JsonProcessingException {
//        JsonWriter writer = new JsonWriter(new StringWriter(180000), jsonWriterSettings);
//        encoder.encode(writer, dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
//        return writer.getWriter().toString();

        encoder.encode(writer, dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return objectMapper.writeValueAsString(writer.getRoot());
    }


}
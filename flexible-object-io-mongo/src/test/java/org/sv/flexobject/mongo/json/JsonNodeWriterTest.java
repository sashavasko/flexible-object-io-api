package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import org.apache.commons.io.IOUtils;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.json.JsonWriterSettings;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;
import static junit.framework.TestCase.assertEquals;


public class JsonNodeWriterTest {

    static ObjectMapper objectMapper = new ObjectMapper();

    public void doTest(String file) throws IOException {
        String simpleReportJson = IOUtils.toString(
                this.getClass().getResourceAsStream(file),
                "UTF-8"
        );
        BasicDBObject dbObject = BasicDBObject.parse(simpleReportJson);

        JsonNodeWriter writer = new JsonNodeWriter(new JsonWriterSettings());
        Encoder encoder = getDefaultCodecRegistry().get(BasicDBObject.class);
        encoder.encode(writer,  dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        JsonNode cleanJson = objectMapper.readTree(dbObject.toJson());

        assertEquals (cleanJson.toString(), writer.getRoot().toString());
        assertEquals (cleanJson.toString(), objectMapper.writeValueAsString(writer.getRoot()));
    }

    @Test
    public void testSimpleReport() throws Exception {
        doTest("simpleReport.json");
    }

    @Test
    public void serviceLink() throws Exception {
        doTest( "serviceLinkReport.json");
    }

    @Test
    public void brandedTwoOwners() throws Exception {
        doTest( "brandedTwoOwners.json");
    }

    @Ignore("This is supposed to fail")
    @Test
    public void error1() throws Exception {
        doTest( "error1.json");
    }


    static JsonWriterSettings jsonWriterSettings = new JsonWriterSettings();
    static Encoder encoder = getDefaultCodecRegistry().get(BasicDBObject.class);
    static JsonNodeWriter writer = new JsonNodeWriter(new JsonWriterSettings());

    public static String basicDBObjectToJson (BasicDBObject dbObject) throws JsonProcessingException {
//        JsonWriter writer = new JsonWriter(new StringWriter(180000), jsonWriterSettings);
//        encoder.encode(writer, dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
//        return writer.getWriter().toString();

        encoder.encode(writer, dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        return objectMapper.writeValueAsString(writer.getRoot());
    }


    @Ignore
    @Test
    public void perfTest() throws Exception {
        String simpleReportJson = IOUtils.toString(
                this.getClass().getResourceAsStream("serviceLinkReport.json"),
                "UTF-8"
        );
        BasicDBObject dbObject = BasicDBObject.parse(simpleReportJson);

        int total1 = 0;
        int total2 = 0;
        int total3 = 0;
        int iterations = 100;
        for (int k = 0 ; k < 3 ; ++k) {
            long start1 = System.currentTimeMillis();
            for (int i = 0; i < iterations; ++i) {
                writer.reset();
                encoder.encode(writer, dbObject, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            }
            long end1 = System.currentTimeMillis();
            total1 += end1-start1;

            long start2 = System.currentTimeMillis();
            for (int i = 0; i < iterations; ++i) {
                String json = basicDBObjectToJson(dbObject);
                objectMapper.readTree(json);
            }

            long end2 = System.currentTimeMillis();
            total2 += end2-start2;

            long start3 = System.currentTimeMillis();
            for (int i = 0; i < iterations; ++i) {
                String json = dbObject.toJson();
                objectMapper.readTree(json);
            }

            long end3 = System.currentTimeMillis();
            total3 += end3-start3;
        }
        System.out.println("fast:" + ((float)total1/(3*iterations)) + ", slow:" + ((float)total2/(3*iterations)) + ", very slow:" + ((float)total3/(3*iterations)));
    }
}
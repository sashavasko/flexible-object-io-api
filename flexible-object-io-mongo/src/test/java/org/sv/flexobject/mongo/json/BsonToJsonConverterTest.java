package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;

import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static junit.framework.TestCase.assertEquals;

public class BsonToJsonConverterTest {

    @Test
    public void convert() throws IOException {
        Bson query = Filters.and(Filters.gte("_id", new ObjectId("609285a90000000000000000")),Filters.lt("_id", new ObjectId("6092868a0000000000000000")));

        JsonNode json = BsonToJsonConverter.relaxed().convert(query);
        System.out.println(query.toBsonDocument().toJson());
        assertEquals(query.toBsonDocument().toJson().replace(" ", ""), json.toString());
    }

    static ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void complexObjectJson() throws Exception {
        TestDataWithInferredSchema testData = TestDataWithInferredSchema.random(true);

        JsonNode json = testData.toJson();
        Document document = Document.parse(json.toString());

        JsonNodeWriter writer = new JsonNodeWriter(new SingleValueSink<JsonNode>(), JsonWriterSettings.builder().build());
        Encoder encoder = getDefaultCodecRegistry().get(Document.class);
        encoder.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        JsonNode actualJson = writer.getSink().get();

        System.out.println(actualJson);
        assertEquals(json.toString(), actualJson.toString());
    }

}
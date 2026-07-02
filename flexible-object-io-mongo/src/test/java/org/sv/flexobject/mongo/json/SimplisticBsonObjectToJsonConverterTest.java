package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SimplisticBsonObjectToJsonConverterTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void convert() throws Exception {

        BasicDBObject dbObject = new BasicDBObject("foo", "bar");
        JsonNode json = SimplisticBsonObjectToJsonConverter.getInstance().convert(dbObject);

        assertEquals("{\"foo\":\"bar\"}", objectMapper.writeValueAsString(json));

    }

}
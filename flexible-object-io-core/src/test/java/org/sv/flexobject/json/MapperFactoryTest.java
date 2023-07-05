package org.sv.flexobject.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jdk.nashorn.internal.ir.ObjectNode;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.testdata.ObjectWithClass;
import org.sv.flexobject.testdata.ObjectWithDate;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class MapperFactoryTest {

    @Mock
    ObjectMapper objectMapper;

    @Mock
    ObjectReader objectReader;

    @Mock
    ObjectWriter objectWriter;

    @After
    public void tearDown() throws Exception {
        MapperFactory.getInstance().reset();
    }

    @Test
    public void getInstance() {
        MapperFactory instance = MapperFactory.getInstance();
        assertSame(instance, MapperFactory.getInstance());
    }

    @Test
    public void getSetObjectMapper() {
        assertNotNull(MapperFactory.getObjectMapper());

        MapperFactory.setObjectMapper(objectMapper);

        assertSame(objectMapper, MapperFactory.getObjectMapper());
    }

    @Test
    public void getSetObjectReader() {
        assertNotNull(MapperFactory.getObjectReader());

        MapperFactory.setObjectReader(objectReader);

        assertSame(objectReader, MapperFactory.getObjectReader());
    }

    @Test
    public void getSetObjectWriter() {
        assertNotNull(MapperFactory.getObjectWriter());

        MapperFactory.setObjectWriter(objectWriter);

        assertSame(objectWriter, MapperFactory.getObjectWriter());
    }

    @Test
    public void reset() {
        MapperFactory.setObjectMapper(objectMapper);
        MapperFactory.setObjectReader(objectReader);
        MapperFactory.setObjectWriter(objectWriter);

        MapperFactory.getInstance().reset();

        assertNotEquals(objectWriter, MapperFactory.getObjectWriter());
        assertNotEquals(objectReader, MapperFactory.getObjectReader());
        assertNotEquals(objectMapper, MapperFactory.getObjectMapper());
    }


    @Test
    public void yaml() throws Exception {
        TestDataWithSubSchemaInCollection testData = TestDataWithSubSchemaInCollection.random(true);
        JsonNode json = testData.toJson();
        String yamlString = MapperFactory.getYamlObjectWriter().writeValueAsString(json);
        JsonNode jsonOut = MapperFactory.getYamlObjectReader().readTree(yamlString);
        TestDataWithSubSchemaInCollection testDataOut = new TestDataWithSubSchemaInCollection();
        testDataOut.fromJson(jsonOut);
        assertEquals(testData, testDataOut);
        assertEquals(json, jsonOut);
    }

    @Test
    public void yamlDate() throws Exception {
        ObjectWithDate testData = ObjectWithDate.random();
        JsonNode json = testData.toJson();
        String yamlString = MapperFactory.getYamlObjectWriter().writeValueAsString(json);
        JsonNode jsonOut = MapperFactory.getYamlObjectReader().readTree(yamlString);
        ObjectWithDate testDataOut = new ObjectWithDate().fromJson(jsonOut);
        assertEquals(testData, testDataOut);
        assertEquals(json, jsonOut);
    }

    @Test
    public void yamlClass() throws Exception {
        ObjectWithClass testData = ObjectWithClass.random();
        JsonNode json = testData.toJson();
        String yamlString = MapperFactory.getYamlObjectWriter().writeValueAsString(json);
//        System.out.println(yamlString);
        JsonNode jsonOut = MapperFactory.getYamlObjectReader().readTree(yamlString);
        ObjectWithClass testDataOut = new ObjectWithClass().fromJson(jsonOut);
        assertEquals(testData, testDataOut);
        assertEquals(json, jsonOut);

        ObjectReader or = MapperFactory.getYamlObjectMapper().readerFor(ObjectWithClass.class);
        ObjectWithClass testDataOutSingle = or.readValue(yamlString);
        assertEquals(testData, testDataOutSingle);

        Map<String, Object> map = MapperFactory.getYamlObjectMapper().readValue(yamlString, new TypeReference<Map<String,Object>>(){});
        ObjectWithClass testDataOutSingleMap = new ObjectWithClass().fromMap(map);
        assertEquals(testData, testDataOutSingleMap);

        ObjectWithDate testData1 = ObjectWithDate.random();
        Thread.sleep(2000);
        ObjectWithDate testData2 = ObjectWithDate.random();
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        MapperFactory.getYamlObjectWriter().writeValue(byteArrayOutputStream, testData1.toJson());
        MapperFactory.getYamlObjectWriter().writeValue(byteArrayOutputStream, testData2.toJson());

        byte[] fullYaml = byteArrayOutputStream.toByteArray();
        System.out.println(new String(fullYaml));
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(fullYaml);

        ObjectReader or2 = MapperFactory.getYamlObjectReader().forType(JsonNode.class);
        Iterator<JsonNode> mapIter = or2.readValues(byteArrayInputStream);
        List<ObjectWithDate> out = new ArrayList<>();
        while (mapIter.hasNext()){
            JsonNode map1 = mapIter.next();
            ObjectWithDate testDataOut1 = new ObjectWithDate().fromJson(map1);
            out.add(testDataOut1);
        }
        assertEquals(testData1, out.get(0));
        assertEquals(testData2, out.get(1));

    }
}
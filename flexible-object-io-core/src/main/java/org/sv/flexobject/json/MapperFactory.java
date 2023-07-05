package org.sv.flexobject.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;

public class MapperFactory {

    private static ObjectMapper objectMapper = new ObjectMapper();
    private static ObjectReader objectReader = objectMapper.reader();
    private static ObjectWriter objectWriter = objectMapper.writer();

    private static ObjectMapper objectMapperYaml = new ObjectMapper(new YAMLFactory());
    private static ObjectReader objectReaderYaml = objectMapperYaml.reader();
    private static ObjectWriter objectWriterYaml = objectMapperYaml.writer();

    private static final MapperFactory instance = new MapperFactory();

    private MapperFactory(){reset();}

    public static MapperFactory getInstance(){
        return instance;
    }

    public void reset(){
        objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader();
        objectWriter = objectMapper.writer();
        objectMapperYaml = new ObjectMapper(new YAMLFactory());
        objectReaderYaml = objectMapperYaml.reader();
        objectWriterYaml = objectMapperYaml.writer();
    }

    public static ObjectMapper getObjectMapper(){
        return  getInstance().objectMapper;
    }

    public static ObjectReader getObjectReader(){
        return getInstance().objectReader;
    }

    public static ObjectWriter getObjectWriter(){
        return getInstance().objectWriter;
    }

    public static ObjectMapper getYamlObjectMapper(){
        return  getInstance().objectMapperYaml;
    }

    public static ObjectReader getYamlObjectReader(){
        return getInstance().objectReaderYaml;
    }

    public static ObjectWriter getYamlObjectWriter(){
        return getInstance().objectWriterYaml;
    }

    public static void setObjectMapper(ObjectMapper objectMapper) {
        MapperFactory.objectMapper = objectMapper;
    }

    public static void setObjectReader(ObjectReader objectReader) {
        MapperFactory.objectReader = objectReader;
    }

    public static void setObjectWriter(ObjectWriter objectWriter) {
        MapperFactory.objectWriter = objectWriter;
    }

    public static void setYamlObjectMapper(ObjectMapper objectMapper) {
        MapperFactory.objectMapperYaml = objectMapper;
    }

    public static void setYamlObjectReader(ObjectReader objectReader) {
        MapperFactory.objectReaderYaml = objectReader;
    }

    public static void setYamlObjectWriter(ObjectWriter objectWriter) {
        MapperFactory.objectWriterYaml = objectWriter;
    }
    public static String pretty(String json) throws IOException {
        JsonNode jsonNode = MapperFactory.getObjectReader().readTree(json);
        return pretty(jsonNode);
    }
    public static String pretty(JsonNode jsonNode) throws JsonProcessingException {
        return MapperFactory.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonNode);
    }

}

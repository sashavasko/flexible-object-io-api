package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

public class MapperFactory {

    private static ObjectMapper objectMapper;
    private static ObjectReader objectReader;
    private static ObjectWriter objectWriter;

    private static MapperFactory instance = null;

    private MapperFactory(){reset();}

    public static MapperFactory getInstance(){
        if(instance == null)
            instance = new MapperFactory();
        return instance;
    }

    public void reset(){
        objectMapper = new ObjectMapper();
        objectReader = objectMapper.reader();
        objectWriter = objectMapper.writer();
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

    public static void setObjectMapper(ObjectMapper objectMapper) {
        MapperFactory.objectMapper = objectMapper;
    }

    public static void setObjectReader(ObjectReader objectReader) {
        MapperFactory.objectReader = objectReader;
    }

    public static void setObjectWriter(ObjectWriter objectWriter) {
        MapperFactory.objectWriter = objectWriter;
    }
}

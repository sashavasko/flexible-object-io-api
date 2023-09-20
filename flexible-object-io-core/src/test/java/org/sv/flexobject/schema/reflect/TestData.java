package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.io.IOException;
import java.util.*;

public class TestData {
    ObjectNode json;
    @ValueType(type = DataTypes.string)
    List<String> listOfStrings = new ArrayList<>();
    String[] arrayOfStrings = new String[5];

    Map<String, Integer> mapOfInts = new HashMap<>();
    @ValueType(type = DataTypes.string)
    Set<String> setOfStrings = new HashSet<>();


    public TestData(String json) throws IOException {
        this.json = (ObjectNode) MapperFactory.getObjectReader().readTree(json);
    }

    public TestData(List<String> listOfStrings) {
        this.listOfStrings = listOfStrings;
    }

    public TestData(String[] arrayOfStrings) {
        this.arrayOfStrings = arrayOfStrings;
    }

    public TestData(Set<String> setOfStrings) {
        this.setOfStrings = setOfStrings;
    }

    public TestData(Map<String, Integer> mapOfInts) {
        this.mapOfInts = mapOfInts;
    }

    public TestData() {
    }

    public JsonNode getJson() {
        return json;
    }

    public List<String> getListOfStrings() {
        return listOfStrings;
    }

    public String[] getArrayOfStrings() {
        return arrayOfStrings;
    }

    public Map<String, Integer> getMapOfInts() {
        return mapOfInts;
    }

    public Set<String> getSetOfStrings() {
        return setOfStrings;
    }
}

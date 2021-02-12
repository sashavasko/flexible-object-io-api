package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.filter2.predicate.FilterApi.*;

public class ParquetFilterParser {

    public static FilterPredicate parse(String jsonString) throws IOException {
        return json2FilterPredicate(new ObjectMapper().readTree(jsonString));
    }

    public static FilterPredicate json2FilterPredicate(JsonNode jsonNode) {
        if (jsonNode.has("or")){
            List<FilterPredicate> predicates = jsonArray2Predicates(jsonNode.get("or"));
            return or(predicates.get(0), predicates.get(1));
        }
        if (jsonNode.has("and")){
            List<FilterPredicate> predicates = jsonArray2Predicates(jsonNode.get("and"));
            return and(predicates.get(0), predicates.get(1));
        }
        if (jsonNode.has("not")){
            FilterPredicate predicate = json2FilterPredicate(jsonNode.get("not"));
            return not(predicate);
        }

        Map.Entry<String, JsonNode> op = jsonNode.fields().next();
        String opName = op.getKey();
        Iterator<Map.Entry<String, JsonNode>> opFields = op.getValue().fields();
        JsonNode valueNode = null;
        String columnType = null;
        String columnName = null;

        while (opFields.hasNext()){
            Map.Entry<String, JsonNode> e = opFields.next();
            if (e.getKey().equals("value")){
                valueNode = e.getValue();
            }else{
                columnType = e.getKey();
                columnName = e.getValue().asText();
            }
        }

        switch(columnType) {
            case "string" :
            case "utf8" :
            case "text" :
            case "binary" : return binaryOp(opName, columnName, valueNode);
            case "int" : return intOp(opName, columnName, valueNode);
            case "long" : return longOp(opName, columnName, valueNode);
            case "bool" :
            case "boolean" : return booleanOp(opName, columnName, valueNode);
            case "double" : return doubleOp(opName, columnName, valueNode);
        }

        throw new RuntimeException("Invalid column type name:" + columnType);
    }

    private static FilterPredicate binaryOp(String opName, String columnName, JsonNode valueNode) {
        switch(opName){
            case "eq" : return eq(binaryColumn(columnName), Binary.fromString(valueNode.asText()));
            case "notEq" : return notEq(binaryColumn(columnName), Binary.fromString(valueNode.asText()));
        }
        throw new RuntimeException("Invalid Filter Predicate Operation name:" + opName);
    }

    private static FilterPredicate intOp(String opName, String columnName, JsonNode valueNode) {
        switch(opName){
            case "eq" : return eq(intColumn(columnName), valueNode.asInt());
            case "notEq" : return notEq(intColumn(columnName), valueNode.asInt());
            case "lt" : return lt(intColumn(columnName), valueNode.asInt());
            case "ltEq" : return ltEq(intColumn(columnName), valueNode.asInt());
            case "gt" : return gt(intColumn(columnName), valueNode.asInt());
            case "gtEq" : return gtEq(intColumn(columnName), valueNode.asInt());
        }
        throw new RuntimeException("Invalid Filter Predicate Operation name:" + opName);
    }

    private static FilterPredicate longOp(String opName, String columnName, JsonNode valueNode) {
        switch(opName){
            case "eq" : return eq(longColumn(columnName), valueNode.asLong());
            case "notEq" : return notEq(longColumn(columnName), valueNode.asLong());
            case "lt" : return lt(longColumn(columnName), valueNode.asLong());
            case "ltEq" : return ltEq(longColumn(columnName), valueNode.asLong());
            case "gt" : return gt(longColumn(columnName), valueNode.asLong());
            case "gtEq" : return gtEq(longColumn(columnName), valueNode.asLong());
        }
        throw new RuntimeException("Invalid Filter Predicate Operation name:" + opName);
    }

    private static FilterPredicate booleanOp(String opName, String columnName, JsonNode valueNode) {
        switch(opName){
            case "eq" : return eq(booleanColumn(columnName), valueNode.asBoolean());
            case "notEq" : return notEq(booleanColumn(columnName), valueNode.asBoolean());
        }
        throw new RuntimeException("Invalid Filter Predicate Operation name:" + opName);
    }

    private static FilterPredicate doubleOp(String opName, String columnName, JsonNode valueNode) {
        switch(opName){
            case "lt" : return lt(doubleColumn(columnName), valueNode.asDouble());
            case "ltEq" : return ltEq(doubleColumn(columnName), valueNode.asDouble());
            case "gt" : return gt(doubleColumn(columnName), valueNode.asDouble());
            case "gtEq" : return gtEq(doubleColumn(columnName), valueNode.asDouble());
        }
        throw new RuntimeException("Invalid Filter Predicate Operation name:" + opName);
    }


    private static List<FilterPredicate> jsonArray2Predicates(JsonNode jsonArray) {
        List<FilterPredicate> predicates = new ArrayList<>();
        for (JsonNode node : jsonArray ){
            predicates.add(json2FilterPredicate(node));
        }
        return predicates;
    }
}

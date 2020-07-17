package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.copy.Copyable;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;

public class JsonInputAdapter extends GenericInAdapter<JsonNode> implements Copyable {
    public static final String JSON_DATE_FORMAT = "MMM dd, yyyy hh:mm:ss aa";
    public static final String JSON_DATE_FORMAT_SHORT = "yyyy-MM-dd";

    public static final DateTimeFormatter jsonDateFormatter = DateTimeFormat.forPattern(JSON_DATE_FORMAT).withZoneUTC();
    public static final DateTimeFormatter jsonDateFormatterShort = DateTimeFormat.forPattern(JSON_DATE_FORMAT_SHORT);

    public JsonInputAdapter(Source<JsonNode> source) {
        super(source);
    }

    @Override
    public String getString(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        if (n == null)
            return null;

        if (n.isValueNode()){
            return n.asText();
        }

        return MapperFactory.getObjectWriter().writeValueAsString(n);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        return getCurrent().get(translateInputFieldName(fieldName));
    }

    @Override
    public Integer getInt(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return n == null ? null : n.asInt();
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return n == null ? null : n.asBoolean();
    }

    @Override
    public Long getLong(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return n == null ? null : n.asLong();
    }

    @Override
    public Double getDouble(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return n == null ? null : n.asDouble();
    }

    public static Date jsonNodeToDate(JsonNode n){
        if (n == null)
            return null;
        String dateString = n.asText();
        DateTimeFormatter formatter = dateString.length() == JSON_DATE_FORMAT_SHORT.length() ? jsonDateFormatterShort: jsonDateFormatter;
        DateTime date = DateTime.parse(dateString, formatter);
        return new Date(date.getMillis());
    }

    @Override
    public Date getDate(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return jsonNodeToDate(n);
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws Exception {
        JsonNode n = getCurrent().get(translateInputFieldName(fieldName));
        return n == null ? null : new Timestamp(n.asLong());
    }

     public static void consume(JsonNode json, ConsumerWithException<JsonInputAdapter, Exception> consumer) throws Exception {
        if (json != null) {
            Source<JsonNode> source = new SingleValueSource<>(json);
            JsonInputAdapter adapter = new JsonInputAdapter(source);
            adapter.next();

            consumer.accept(adapter);
        }
    }

    @Override
    public void copyRecord(CopyAdapter to) {
        ObjectNode current = (ObjectNode) getCurrent();
        Iterator<Map.Entry<String, JsonNode>> fields = current.fields();
        while (fields.hasNext()){
            Map.Entry<String, JsonNode> field = fields.next();
            switch(field.getValue().getNodeType()){
                case BOOLEAN: to.put(field.getKey(), field.getValue().asBoolean()); break;
                case NUMBER: to.put(field.getKey(), field.getValue().asLong()); break;
                case STRING: to.put(field.getKey(), field.getValue().asText()); break;
                case OBJECT: to.put(field.getKey(), field.getValue()); break;
            }
        }
    }
}

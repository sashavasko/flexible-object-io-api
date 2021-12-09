package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.function.Supplier;

public class JsonOutputAdapter extends GenericOutAdapter<ObjectNode> {
    public JsonOutputAdapter() {
        super(JsonNodeFactory.instance::objectNode);
    }

    public JsonOutputAdapter(Sink<ObjectNode> sink) {
        super(sink, JsonNodeFactory.instance::objectNode);
    }

    public JsonOutputAdapter(Supplier<ObjectNode> recordFactory) {
        super(recordFactory);
    }

    public JsonOutputAdapter(Sink sink, Supplier<ObjectNode> recordFactory) {
        super(sink, recordFactory);
    }

    @Override
    public void setString(String paramName, String value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value);
    }

    @Override
    public void setJson(String paramName, JsonNode value) {
        if (value != null && value.size() > 0) {
            getCurrent().set(translateOutputFieldName(paramName), value);
        }
    }

    @Override
    public void setInt(String paramName, Integer value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value);
    }

    @Override
    public void setBoolean(String paramName, Boolean value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value);
    }

    @Override
    public void setLong(String paramName, Long value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value);
    }

    @Override
    public void setDouble(String paramName, Double value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value);
    }

    public static String formatDate(Date date){
        return new DateTime(date.getTime(), DateTimeZone.UTC).toString(JsonInputAdapter.JSON_DATE_FORMAT);
    }

    public static JsonNode dateToJsonNode(Object value){
        return JsonNodeFactory.instance.textNode(formatDate((Date)value));
    }

    public static JsonNode localDateToJsonNode(Object value){
        return dateToJsonNode(Date.valueOf((LocalDate)value));
    }

    @Override
    public void setDate(String paramName, Date value) {
        if (value != null)
            getCurrent().set(translateOutputFieldName(paramName), dateToJsonNode(value));
    }

    public static JsonNode timestampToJsonNode(Object value){
        return JsonNodeFactory.instance.numberNode(((Timestamp)value).getTime());
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) {
        if (value != null)
            getCurrent().set(translateOutputFieldName(paramName), timestampToJsonNode(value));
    }

    @Override
    public JsonOutputAdapter save() throws Exception {
        return (JsonOutputAdapter) super.save();
    }

    public static ObjectNode produce(Streamable data) throws Exception {
        return produce(data::save);
    }

    public static ObjectNode produce(ConsumerWithException<JsonOutputAdapter, Exception> consumer) throws Exception {
        SingleValueSink<ObjectNode> sink = new SingleValueSink<>();
        JsonOutputAdapter adapter = new JsonOutputAdapter(sink);

        consumer.accept(adapter);

        return sink.get();
    }

}

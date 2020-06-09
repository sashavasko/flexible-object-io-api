package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;

public class JsonOutputAdapter extends GenericOutAdapter<ObjectNode> {

    public JsonOutputAdapter() {
        super();
    }

    public JsonOutputAdapter(Sink<ObjectNode> sink) {
        super(sink);
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
    public void setDate(String paramName, Date value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), new DateTime(value.getTime(), DateTimeZone.UTC).toString(JsonInputAdapter.JSON_DATE_FORMAT));
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) {
        if (value != null)
            getCurrent().put(translateOutputFieldName(paramName), value.getTime());
    }

    @Override
    public ObjectNode createRecord() {
        return JsonNodeFactory.instance.objectNode();
    }

    @Override
    public JsonOutputAdapter save() throws Exception {
        return (JsonOutputAdapter) super.save();
    }

    public static ObjectNode produce(ConsumerWithException<JsonOutputAdapter, Exception> consumer) throws Exception {
        SingleValueSink<ObjectNode> sink = new SingleValueSink<>();
        JsonOutputAdapter adapter = new JsonOutputAdapter(sink);

        consumer.accept(adapter);

        return sink.get();
    }

}

package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;

public class AvroOutputAdapter extends GenericOutAdapter<GenericRecord> {

    public static final String PARAM_SCHEMA = "schema";

    Schema schema;

    public AvroOutputAdapter(Schema schema, Sink<GenericRecord> sink) {
        super(sink);
        this.schema = schema;
    }

    public AvroOutputAdapter(Sink<GenericRecord> sink) {
        super(sink);
    }

    @Override
    public void setParam(String key, Object value) {
        if (PARAM_SCHEMA.equals(key))
            schema = (Schema) value;
    }

    @Override
    public GenericRecord createRecord() {
        return new GenericData.Record(schema);
    }

    protected void put(String paramName, Object value){
        String translatedName = translateOutputFieldName(paramName);
        if (translatedName != null && value != null)
            getCurrent().put(translatedName, value);
    }

    @Override
    public void setString(String paramName, String value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setJson(String paramName, JsonNode value) throws Exception {
        if (value != null)
            put(paramName, value.toString());
    }

    @Override
    public void setInt(String paramName, Integer value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setBoolean(String paramName, Boolean value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setLong(String paramName, Long value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setDouble(String paramName, Double value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setDate(String paramName, Date value) throws Exception {
        if(value!= null)
            put(paramName, new DateTime(value.getTime(), DateTimeZone.UTC).toString(JsonInputAdapter.JSON_DATE_FORMAT));
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) throws Exception {
        if(value!= null)
            put(paramName, value.getTime());
    }

    public static GenericRecord produce(Schema schema, ConsumerWithException<AvroOutputAdapter, Exception> consumer) throws Exception {
        SingleValueSink<GenericRecord> sink = new SingleValueSink<>();
        AvroOutputAdapter adapter = new AvroOutputAdapter(schema, sink);

        consumer.accept(adapter);

        return sink.get();
    }

    @Override
    public String translateOutputFieldName(String fieldName) {
        if(fieldName != null && schema.getField(fieldName) != null)
            return fieldName;
        return null;
    }
}
package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public class AvroInputAdapter extends GenericInAdapter<GenericRecord> {
    Schema schema;
    public static final String PARAM_SCHEMA = "schema";

    public AvroInputAdapter(Schema schema, Source<GenericRecord> source) {
        super(source);
        this.schema = schema;
    }

    public AvroInputAdapter(Source<GenericRecord> source) {
        super(source);
    }

    @Override
    public void setParam(String key, Object value) {
        if (PARAM_SCHEMA.equals(key))
            schema = (Schema) value;
    }

    Object getField(String fieldName) {
        String translatedFieldName = translateInputFieldName(fieldName);
        return translatedFieldName == null ? null : getCurrent().get(translatedFieldName);
    }

    @Override
    public String getString(String fieldName) throws Exception {
        Object data = getField(fieldName);
        if (data == null)
            return null;

        if (data instanceof Utf8) {
            Utf8 utf8 = (Utf8)data;
            return utf8 == null || utf8.length() == 0 ? null : utf8.toString();
        }

        return DataTypes.stringConverter(data);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        String jsonString = getString(fieldName);
        return jsonString == null ? null : MapperFactory.getObjectReader().readTree(jsonString);
    }

    @Override
    public Integer getInt(String fieldName) throws Exception {
        return (Integer) getField(fieldName);
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        return (Boolean) getField(fieldName);
    }

    @Override
    public Long getLong(String fieldName) throws Exception {
        return (Long) getField(fieldName);
    }

    @Override
    public Double getDouble(String fieldName) throws Exception {
        return (Double) getField(fieldName);
    }

    @Override
    public Date getDate(String fieldName) throws Exception {
        Object value = getField(fieldName);
        if (value == null)
            return null;

        if (value instanceof String) {
            DateTime dt = DateTime.parse(((String)value), DateTimeFormat.forPattern(JsonInputAdapter.JSON_DATE_FORMAT));

            LocalDate ld = LocalDate.of(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth());
            return Date.valueOf(ld);
        } else
            return DataTypes.dateConverter(value);
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws Exception {
        Long timestamp = getLong(fieldName);
        if (timestamp == null)
            return null;

        return new Timestamp(timestamp);
    }

    public static void consume(Schema schema, GenericRecord avro, ConsumerWithException<AvroInputAdapter, Exception> consumer) throws Exception {
        if (avro != null) {
            SingleValueSource<GenericRecord> source = new SingleValueSource<>(avro);
            AvroInputAdapter adapter = new AvroInputAdapter(schema, source);
            adapter.next();

            consumer.accept(adapter);
        }
    }

    @Override
    public String translateInputFieldName(String fieldName) {
        if (fieldName != null && schema.getField(fieldName) != null)
            return fieldName;
        return null;
    }
}

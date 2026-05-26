package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.function.Supplier;

public class JsonOutputAdapter extends GenericOutAdapter<ObjectNode> {
    public JsonOutputAdapter() {
        super(JsonNodeFactory.instance::objectNode);
    }

    public JsonOutputAdapter(Sink<ObjectNode> sink) {
        super(sink, JsonNodeFactory.instance::objectNode);
    }

    public JsonOutputAdapter(Sink<ObjectNode> sink, Function<Date, JsonNode> dateFormatter) {
        super(sink, JsonNodeFactory.instance::objectNode);
        this.dateFormatter = dateFormatter;
    }

    public Function<Date, JsonNode> getDateFormatter() {
        return dateFormatter;
    }

    public void setDateFormatter(Function<Date, JsonNode> dateFormatter) {
        this.dateFormatter = dateFormatter;
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

    public static String formatDate(java.sql.Date date){
        ZonedDateTime zdt = Instant.ofEpochMilli(date.getTime())
                .atZone(DataTypes.getDefaultZoneId());
//        System.out.println(ldt);
//        Timestamp timestamp = new Timestamp(date.getTime());
//        System.out.println(timestamp);
//        ZonedDateTime zdt = ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC);
        return JsonInputAdapter.jsonDateFormatter.format(zdt);
        //new DateTime(date.getTime(), DateTimeZone.UTC).toString();
    }

    public static String formatDateUsingLegacyFormat(java.sql.Date date){
        ZonedDateTime zdt = Instant.ofEpochMilli(date.getTime())
                .atZone(ZoneOffset.UTC);
//        System.out.println(ldt);
//        Timestamp timestamp = new Timestamp(date.getTime());
//        System.out.println(timestamp);
//        ZonedDateTime zdt = ZonedDateTime.ofInstant(timestamp.toInstant(), ZoneOffset.UTC);
        return JsonInputAdapter.jsonDateFormatterLegacy.format(zdt);
        //new DateTime(date.getTime(), DateTimeZone.UTC).toString();
    }

    Function<Date, JsonNode> dateFormatter = JsonOutputAdapter::dateToJsonNode;

    public static JsonNode dateToJsonNode(Object value){
        return JsonNodeFactory.instance.textNode(formatDate((Date)value));
    }
    public static JsonNode dateToJsonNodeUsingLegacyFormat(Object value){
        return JsonNodeFactory.instance.textNode(formatDateUsingLegacyFormat((Date)value));
    }

    public static JsonNode localDateToJsonNode(Object value){
        return dateToJsonNode(Date.valueOf((LocalDate)value));
    }

    @Override
    public void setDate(String paramName, java.sql.
            Date value) {
        if (value != null)
            getCurrent().set(translateOutputFieldName(paramName), dateFormatter.apply(value));
    }

    @Override
    public void setDate(String paramName, LocalDate value) {
        if (value != null)
            getCurrent().set(translateOutputFieldName(paramName), JsonNodeFactory.instance.textNode(DateTimeFormatter.ISO_DATE.format(value)));
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

    public static ObjectNode produce(Streamable data, boolean useLegacyDateFormat) throws Exception {
        return produce(data::save, useLegacyDateFormat);
    }

    public static ObjectNode produce(ConsumerWithException<JsonOutputAdapter, Exception> consumer) throws Exception {
        return produce(consumer, false);
    }

    public static ObjectNode produce(ConsumerWithException<JsonOutputAdapter, Exception> consumer, boolean useLegacyDateFormat) throws Exception {
        return produce(consumer, useLegacyDateFormat ? JsonOutputAdapter::dateToJsonNodeUsingLegacyFormat : JsonOutputAdapter::dateToJsonNode);
    }

    public static ObjectNode produce(ConsumerWithException<JsonOutputAdapter, Exception> consumer, Function<Date, JsonNode> dateFormatter) throws Exception {
        SingleValueSink<ObjectNode> sink = new SingleValueSink<>();
        JsonOutputAdapter adapter = new JsonOutputAdapter(sink, dateFormatter);

        consumer.accept(adapter);

        return sink.get();
    }

}

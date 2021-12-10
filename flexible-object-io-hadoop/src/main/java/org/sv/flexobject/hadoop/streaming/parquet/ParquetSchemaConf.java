package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.mapreduce.input.parquet.JsonParquetInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.parquet.StreamableParquetInputFormat;
import org.sv.flexobject.hadoop.mapreduce.output.parquet.JsonParquetOutputFormat;
import org.sv.flexobject.hadoop.mapreduce.output.parquet.StreamableParquetOutputFormat;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.properties.Namespace;

import java.io.IOException;
import java.util.List;

public final class ParquetSchemaConf extends HadoopPropertiesWrapper<ParquetSchemaConf> {
    public static final String SUBNAMESPACE = "parquet";

    Class<?> inputSchemaClass;
    Class<?> outputSchemaClass;
    JsonNode inputSchemaJson;
    JsonNode outputSchemaJson;
    JsonNode filterPredicateJson;

    public ParquetSchemaConf() {
        super(SUBNAMESPACE);
    }

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    @Override
    public ParquetSchemaConf setDefaults() {
        return this;
    }

    public ParquetSchemaConf(Namespace namespace) {
        super(namespace, SUBNAMESPACE);
    }

    public MessageType getInputSchema(){
        return getSchema(inputSchemaClass, inputSchemaJson);
    }

    public MessageType getOutputSchema(){
        return getSchema(outputSchemaClass, outputSchemaJson);
    }

    public boolean hasInputSchema(){
        return inputSchemaClass != null || inputSchemaJson != null;
    }

    public boolean hasOutputSchema(){
        return outputSchemaClass != null || outputSchemaJson != null;
    }

    public ParquetSchemaConf setInputSchemaClass(Class<?> inputSchemaClass) {
        this.inputSchemaClass = inputSchemaClass;
        return this;
    }

    public ParquetSchemaConf setOutputSchemaClass(Class<?> outputSchemaClass) {
        this.outputSchemaClass = outputSchemaClass;
        return this;
    }

    public ParquetSchemaConf setInputSchemaJson(JsonNode inputSchemaJson) {
        this.inputSchemaJson = inputSchemaJson;
        return this;
    }

    public ParquetSchemaConf setInputSchemaJson(String inputSchemaJson) throws IOException {
        this.inputSchemaJson = StringUtils.isBlank(inputSchemaJson) ? null : MapperFactory.getObjectReader().readTree(inputSchemaJson);
        return this;
    }

    public ParquetSchemaConf setOutputSchemaJson(JsonNode outputSchemaJson) {
        this.outputSchemaJson = outputSchemaJson;
        return this;
    }

    public ParquetSchemaConf setOutputSchemaJson(String outputSchemaJson) throws IOException {
        this.outputSchemaJson = StringUtils.isBlank(outputSchemaJson) ? null : MapperFactory.getObjectReader().readTree(outputSchemaJson);
        return this;
    }

    public static MessageType getSchema(Class<?> schemaClass, JsonNode schemaJson){
        if (schemaClass != null) {
            return ParquetSchema.forClass(schemaClass);
        } else if (schemaJson != null) {
            try {
                if (schemaJson.isArray()){
                    List<Type> fields = ParquetSchema.fromJson((ArrayNode) schemaJson);
                    return new MessageType("jsonSchema", fields);
                } else {
                    return ParquetSchema.fromJson((ObjectNode) schemaJson);
                }
            } catch (Exception e) {
                throw new RuntimeException("failed to parse Parquet schema JSON: \"" + schemaJson + "\"", e);
            }
        }
        return null;
    }

    public Class<? extends InputFormat> getInputFormat() {
        if (inputSchemaClass != null) {
            return StreamableParquetInputFormat.class;
        } else if (inputSchemaJson != null) {
            return JsonParquetInputFormat.class;
        }
        return JsonParquetInputFormat.class;
    }

    public Class<? extends OutputFormat> getOutputFormat() {
        if (outputSchemaClass != null)
            return StreamableParquetOutputFormat.class;
        else
            return JsonParquetOutputFormat.class;
    }

    public Class<? extends Writable> getOutputClass() {
        // this should be fine as Parquet does not use actual methods from Writable:
        return (Class<? extends Writable>)(outputSchemaClass == null ?  JsonNode.class : outputSchemaClass);
    }

    public FilterPredicate getFilterPredicate() {
        FilterPredicate filter = null;
        if (filterPredicateJson != null)
            filter = ParquetFilterParser.json2FilterPredicate(filterPredicateJson);
        return filter;
    }

    public ParquetSchemaConf setFilterPredicate(String predicateJson) throws JsonProcessingException {
        this.filterPredicateJson = MapperFactory.getObjectReader().readTree(predicateJson);
        return this;
    }

    public boolean hasFilterPredicate() {
        return filterPredicateJson != null  && filterPredicateJson.size() != 0;
    }
}

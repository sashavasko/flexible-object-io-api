package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.*;
import org.bson.json.Converter;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.json.StrictJsonWriter;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.joda.time.DateTime;
import org.sv.flexobject.stream.Sink;

import java.util.Stack;


public class JsonNodeWriter extends AbstractBsonWriter {
    private final JsonWriterSettings settings;
    private final StrictJsonNodeWriter strictJsonWriter;
    private Sink<JsonNode> sink;

    /**
     * Creates a new instance which uses {@code writer} to write JSON to.
     *
     * @param sink the writer to write JSON to.
     */
    public JsonNodeWriter(final Sink<JsonNode> sink) {
        this(sink, JsonWriterSettings.builder().build());
    }



    /**
     * Creates a new instance which uses {@code writer} to write JSON to and uses the given settings.
     *
     * @param sink   the sink to write JSON to.
     * @param settings the settings to apply to this writer.
     */
    public JsonNodeWriter(final Sink<JsonNode> sink, final JsonWriterSettings settings) {
        super(settings);
        this.sink = sink;
        this.settings = settings;
        setContext(new JsonNodeWriter.Context(null, BsonContextType.TOP_LEVEL));
        strictJsonWriter = new StrictJsonNodeWriter(sink);
    }

    /**
     * Gets the {@code Writer}.
     *
     * @return the writer
     */
    public Sink<JsonNode> getSink() {
        return sink;
    }

    @Override
    protected Context getContext() {
        return (Context) super.getContext();
    }

    @Override
    protected void doWriteName(final String name) {
        strictJsonWriter.writeName(name);
    }

    @Override
    protected void doWriteStartDocument() {
        strictJsonWriter.writeStartObject();

        BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
        setContext(new JsonNodeWriter.Context(getContext(), contextType));
    }

    @Override
    protected void doWriteEndDocument() {
        strictJsonWriter.writeEndObject();
        if (getContext().getContextType() == BsonContextType.SCOPE_DOCUMENT) {
            setContext(getContext().getParentContext());
            writeEndDocument();
        } else {
            setContext(getContext().getParentContext());
        }
    }

    @Override
    protected void doWriteStartArray() {
        strictJsonWriter.writeStartArray();
        setContext(new JsonNodeWriter.Context(getContext(), BsonContextType.ARRAY));
    }

    @Override
    protected void doWriteEndArray() {
        strictJsonWriter.writeEndArray();
        setContext(getContext().getParentContext());
    }


    @Override
    protected void doWriteBinaryData(final BsonBinary binary) {
        settings.getBinaryConverter().convert(binary, strictJsonWriter);
    }

    @Override
    public void doWriteBoolean(final boolean value) {
        settings.getBooleanConverter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteDateTime(final long value) {
        settings.getDateTimeConverter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteDBPointer(final BsonDbPointer value) {
        if (settings.getOutputMode() == JsonMode.EXTENDED) {
            new Converter<BsonDbPointer>() {
                @Override
                public void convert(final BsonDbPointer value1, final StrictJsonWriter writer) {
                    writer.writeStartObject();
                    writer.writeStartObject("$dbPointer");
                    writer.writeString("$ref", value1.getNamespace());
                    writer.writeName("$id");
                    doWriteObjectId(value1.getId());
                    writer.writeEndObject();
                    writer.writeEndObject();
                }
            }.convert(value, strictJsonWriter);
        } else {
            new Converter<BsonDbPointer>() {
                @Override
                public void convert(final BsonDbPointer value1, final StrictJsonWriter writer) {
                    writer.writeStartObject();
                    writer.writeString("$ref", value1.getNamespace());
                    writer.writeName("$id");
                    doWriteObjectId(value1.getId());
                    writer.writeEndObject();
                }
            }.convert(value, strictJsonWriter);
        }
    }

    @Override
    protected void doWriteDouble(final double value) {
        settings.getDoubleConverter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteInt32(final int value) {
        settings.getInt32Converter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteInt64(final long value) {
        settings.getInt64Converter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteDecimal128(final Decimal128 value) {
        settings.getDecimal128Converter().convert(value, strictJsonWriter);
    }

    @Override
    protected void doWriteJavaScript(final String code) {
        settings.getJavaScriptConverter().convert(code, strictJsonWriter);
    }

    @Override
    protected void doWriteJavaScriptWithScope(final String code) {
        writeStartDocument();
        writeString("$code", code);
        writeName("$scope");
    }

    @Override
    protected void doWriteMaxKey() {
        settings.getMaxKeyConverter().convert(null, strictJsonWriter);
    }

    @Override
    protected void doWriteMinKey() {
        settings.getMinKeyConverter().convert(null, strictJsonWriter);
    }

    @Override
    public void doWriteNull() {
        settings.getNullConverter().convert(null, strictJsonWriter);
    }

    @Override
    public void doWriteObjectId(final ObjectId objectId) {
        settings.getObjectIdConverter().convert(objectId, strictJsonWriter);
    }

    @Override
    public void doWriteRegularExpression(final BsonRegularExpression regularExpression) {
        settings.getRegularExpressionConverter().convert(regularExpression, strictJsonWriter);
    }

    @Override
    public void doWriteString(final String value) {
        settings.getStringConverter().convert(value, strictJsonWriter);
    }

    @Override
    public void doWriteSymbol(final String value) {
        settings.getSymbolConverter().convert(value, strictJsonWriter);
    }

    @Override
    public void doWriteTimestamp(final BsonTimestamp value) {
        settings.getTimestampConverter().convert(value, strictJsonWriter);
    }

    @Override
    public void doWriteUndefined() {
        settings.getUndefinedConverter().convert(null, strictJsonWriter);
    }

    @Override
    public void flush() {
        strictJsonWriter.flush();
    }

    /**
     * Return true if the output has been truncated due to exceeding the length specified in {@link JsonWriterSettings#getMaxLength()}.
     *
     * @return true if the output has been truncated
     * @see JsonWriterSettings#getMaxLength()
     * @since 3.7
     */
    public boolean isTruncated() {
        return strictJsonWriter.isTruncated();
    }

    @Override
    protected boolean abortPipe() {
        return strictJsonWriter.isTruncated();
    }

    /**
     * The context for the writer, inheriting all the values from {@link org.bson.AbstractBsonWriter.Context}, and additionally providing
     * settings for the indentation level and whether there are any child elements at this level.
     */
    public class Context extends AbstractBsonWriter.Context {

        /**
         * Creates a new context.
         *
         * @param parentContext the parent context that can be used for going back up to the parent level
         * @param contextType   the type of this context
         */
        public Context(final Context parentContext, final BsonContextType contextType) {
            super(parentContext, contextType);
        }

        @Override
        public Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }
}

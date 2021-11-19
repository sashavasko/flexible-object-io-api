package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.json.StrictJsonWriter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.Sink;

import java.util.Stack;

public class StrictJsonNodeWriter implements StrictJsonWriter {

    protected final Sink<JsonNode> sink;

    Stack<JsonNode> nodeStack = new Stack<>();
    JsonNode currentDocument = null;
    String currentName = null;

    public StrictJsonNodeWriter(final Sink<JsonNode> sink) {
        this.sink = sink;
    }

    protected void put (JsonNode node){
        put(currentName, node);
    }

    protected void put (String name, JsonNode node){
        if (currentDocument != null && currentDocument != node) {
            if (currentDocument instanceof ArrayNode)
                ((ArrayNode) currentDocument).add(node);
            else if (name != null)
                ((ObjectNode) currentDocument).set(name, node);
        }
    }

    protected JsonNode newNode(JsonNode node){
        if (currentDocument != null) {
            nodeStack.push(currentDocument);
        }
        currentDocument = node;
        return node;
    }

    protected void endNode(){
        if(!nodeStack.isEmpty()) {
            currentDocument = nodeStack.pop();
        } else if (currentDocument != null) {
            try {
                sink.put(currentDocument);
                currentDocument = null;
            } catch (Exception e) {
                throw new RuntimeException("Failed to store current document", e);
            }
        }
    }

    @Override
    public void writeName(String name) {
        currentName = name;
    }

    @Override
    public void writeBoolean(boolean value) {
        put(JsonNodeFactory.instance.booleanNode(value));
    }

    @Override
    public void writeBoolean(String name, boolean value) {
        put(name, JsonNodeFactory.instance.booleanNode(value));
    }

    @Override
    public void writeNumber(String value) {
        try {
            put(JsonNodeFactory.instance.numberNode(Long.valueOf(value)));
        }catch(NumberFormatException e){
            put(JsonNodeFactory.instance.numberNode(Double.valueOf(value)));
        }
    }

    @Override
    public void writeNumber(String name, String value) {
        put(name, JsonNodeFactory.instance.numberNode(Long.valueOf(value)));
    }

    StringBuilder sb = new StringBuilder();
    private String escapeString(final String str) {
        sb.setLength(0);
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    switch (Character.getType(c)) {
                        case Character.UPPERCASE_LETTER:
                        case Character.LOWERCASE_LETTER:
                        case Character.TITLECASE_LETTER:
                        case Character.OTHER_LETTER:
                        case Character.DECIMAL_DIGIT_NUMBER:
                        case Character.LETTER_NUMBER:
                        case Character.OTHER_NUMBER:
                        case Character.SPACE_SEPARATOR:
                        case Character.CONNECTOR_PUNCTUATION:
                        case Character.DASH_PUNCTUATION:
                        case Character.START_PUNCTUATION:
                        case Character.END_PUNCTUATION:
                        case Character.INITIAL_QUOTE_PUNCTUATION:
                        case Character.FINAL_QUOTE_PUNCTUATION:
                        case Character.OTHER_PUNCTUATION:
                        case Character.MATH_SYMBOL:
                        case Character.CURRENCY_SYMBOL:
                        case Character.MODIFIER_SYMBOL:
                        case Character.OTHER_SYMBOL:
                            sb.append(c);
                            break;
                        default:
                            sb.append("\\u");
                            sb.append(Integer.toHexString((c & 0xf000) >> 12));
                            sb.append(Integer.toHexString((c & 0x0f00) >> 8));
                            sb.append(Integer.toHexString((c & 0x00f0) >> 4));
                            sb.append(Integer.toHexString(c & 0x000f));
                            break;
                    }
                    break;
            }
        }
        return sb.toString();
    }


    @Override
    public void writeString(String value) {
        put(JsonNodeFactory.instance.textNode(escapeString(value)));
    }

    @Override
    public void writeString(String name, String value) {
        put(name, JsonNodeFactory.instance.textNode(escapeString(value)));
    }

    @Override
    public void writeRaw(String value) {
        put(JsonNodeFactory.instance.textNode(value));
    }

    @Override
    public void writeRaw(String name, String value) {
        put(name, JsonNodeFactory.instance.textNode(value));
    }

    @Override
    public void writeNull() {
        put(JsonNodeFactory.instance.nullNode());
    }

    @Override
    public void writeNull(String name) {
        put(name, JsonNodeFactory.instance.nullNode());
    }

    @Override
    public void writeStartArray() {
        writeStartArray(currentName);
    }

    @Override
    public void writeStartArray(String name) {
        JsonNode node;
        if (currentDocument == null){
            node = DataTypes.jsonArrayNode();
        } else if (currentDocument instanceof ObjectNode) {
            node = ((ObjectNode)currentDocument).putArray(name);
        } else if (currentDocument instanceof ArrayNode) {
            node = ((ArrayNode)currentDocument).addArray();
        } else
            throw new RuntimeException("Bad current document type: " + currentDocument.getClass());
        newNode(node);
    }

    @Override
    public void writeStartObject() {
        writeStartObject(currentName);
    }

    @Override
    public void writeStartObject(String name) {
        JsonNode node;
        if (currentDocument == null){
            node = JsonNodeFactory.instance.objectNode();
        } else if (currentDocument instanceof ObjectNode) {
            node = ((ObjectNode)currentDocument).putObject(name);
        } else if (currentDocument instanceof ArrayNode) {
            node = ((ArrayNode)currentDocument).addObject();
        } else
            throw new RuntimeException("Bad current document type: " + currentDocument.getClass());
        newNode(node);
    }

    @Override
    public void writeEndArray() {
        endNode();
    }

    @Override
    public void writeEndObject() {
        endNode();
    }

    @Override
    public boolean isTruncated() {
        return false;
    }

    public void flush() {
        endNode();
    }
}

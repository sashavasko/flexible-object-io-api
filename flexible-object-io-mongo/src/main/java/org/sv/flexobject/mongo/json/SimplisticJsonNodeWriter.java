package org.sv.flexobject.mongo.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.schema.DataTypes;

import java.sql.Date;
import java.util.Stack;


public class SimplisticJsonNodeWriter extends AbstractBsonWriter {

    JsonNode root = null;
    Stack<JsonNode> nodeStack = new Stack<>();
    JsonNode currentDocument = null;

    public SimplisticJsonNodeWriter(BsonWriterSettings settings) {
        super(settings);
    }

    public JsonNode getRoot() {
        return root;
    }

    protected void addToCurrentDocument (JsonNode node){
        if (currentDocument != null) {
            if (currentDocument instanceof ArrayNode)
                ((ArrayNode) currentDocument).add(node);
            else
                ((ObjectNode) currentDocument).set(getName(), node);
        }
    }

    protected void newNode(JsonNode node){
        if (getState() == State.VALUE || getState() == State.SCOPE_DOCUMENT) {
            addToCurrentDocument(node);
        }
        if (currentDocument != null)
            nodeStack.push(currentDocument);
        else
            root = node;
        currentDocument = node;

    }

    @Override
    protected void doWriteStartDocument() {
        newNode (JsonNodeFactory.instance.objectNode());
        BsonContextType contextType = (getState() == State.SCOPE_DOCUMENT) ? BsonContextType.SCOPE_DOCUMENT : BsonContextType.DOCUMENT;
        setContext(new Context(getContext(), contextType));

    }

    @Override
    protected void doWriteEndDocument() {
        if(!nodeStack.isEmpty()) {
            currentDocument = nodeStack.pop();
        }
        if (getContext().getContextType() == BsonContextType.SCOPE_DOCUMENT) {
            setContext(getContext().getParentContext());
            writeEndDocument();
        } else {
            setContext(getContext().getParentContext());
        }

    }

    @Override
    protected void doWriteStartArray() {
        newNode (DataTypes.jsonArrayNode());
        setContext(new Context(getContext(), BsonContextType.ARRAY));
    }

    @Override
    protected void doWriteEndArray() {
        currentDocument = nodeStack.pop();
        setContext(getContext().getParentContext());
    }

    @Override
    protected void doWriteBinaryData(BsonBinary value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support binary data");
    }

    @Override
    protected void doWriteBoolean(boolean value) {
        addToCurrentDocument(JsonNodeFactory.instance.booleanNode(value));
    }

    @Override
    protected void doWriteDateTime(long value) {
        addToCurrentDocument(JsonNodeFactory.instance.textNode(JsonOutputAdapter.formatDate(new Date(value))));
    }

    @Override
    protected void doWriteDBPointer(BsonDbPointer value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support DBPointer");
    }

    @Override
    protected void doWriteDouble(double value) {
        addToCurrentDocument(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    protected void doWriteInt32(int value) {
        addToCurrentDocument(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    protected void doWriteInt64(long value) {
        addToCurrentDocument(JsonNodeFactory.instance.numberNode(value));
    }

    @Override
    protected void doWriteDecimal128(Decimal128 value) {
        throw new UnsupportedOperationException("Converting 128 bit integer from Mongo to Json not implemented");
    }

    @Override
    protected void doWriteJavaScript(String value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support JavaScript");
    }

    @Override
    protected void doWriteJavaScriptWithScope(String value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support JavaScript with scope");
    }

    @Override
    protected void doWriteMaxKey() {
        throw new UnsupportedOperationException("Json converter for Mongo does not support MAX KEY");
    }

    @Override
    protected void doWriteMinKey() {
        throw new UnsupportedOperationException("Json converter for Mongo does not support MIN KEY");
    }

    @Override
    protected void doWriteNull() {
        addToCurrentDocument(JsonNodeFactory.instance.nullNode());
    }

    @Override
    protected void doWriteObjectId(ObjectId value) {
// todo : wrap ObjectId in {"$oid": "6092868a0000000000000000"}
//        ObjectNode oidNode = JsonNodeFactory.instance.objectNode();
//        oidNode.put("$oid", value.toHexString());
//        addToCurrentDocument(oidNode);
        addToCurrentDocument(JsonNodeFactory.instance.textNode(value.toHexString()));
    }

    @Override
    protected void doWriteRegularExpression(BsonRegularExpression value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support Regular Expressions");
    }

    @Override
    protected void doWriteString(String value) {
        addToCurrentDocument(JsonNodeFactory.instance.textNode(value));
    }

    @Override
    protected void doWriteSymbol(String value) {
        throw new UnsupportedOperationException("Json converter for Mongo does not support Symbols");
    }

    @Override
    protected void doWriteTimestamp(BsonTimestamp value) {
        addToCurrentDocument(JsonNodeFactory.instance.textNode(JsonOutputAdapter.formatDate(new Date(value.getValue()))));
    }

    @Override
    protected void doWriteUndefined() {

    }

    @Override
    public void flush() {

    }

    public void reset() {
        root = null;
        currentDocument = null;
        nodeStack.clear();
    }

    public class Context extends AbstractBsonWriter.Context {

        public Context(final AbstractBsonWriter.Context parentContext, final BsonContextType contextType) {
            super(parentContext, contextType);
        }

        @Override
        public Context getParentContext() {
            return (Context) super.getParentContext();
        }
    }

}

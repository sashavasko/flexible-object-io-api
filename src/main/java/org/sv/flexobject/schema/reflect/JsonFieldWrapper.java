package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import java.util.Arrays;

public class JsonFieldWrapper extends FieldWrapper {

    private String[] path;
    protected String jsonFieldName;

    public JsonFieldWrapper(Class<?> clazz, String fieldName, String pathString) {
        super(clazz, fieldName);
        this.path = pathString.split("\\.");
        jsonFieldName = this.path[this.path.length-1];
        this.path = Arrays.copyOf(this.path, this.path.length-1);
    }

    protected ValueNode findValueInPath(ObjectNode owner, int idx){
        JsonNode node = findNode(owner, 0, false);
        return node == null ? null : (ValueNode) node.get(jsonFieldName);
    }

    protected JsonNode findNode(ObjectNode owner, int idx, boolean createIfMissing){
        String name = path[idx];
        if (!owner.has(name)) {
            if (createIfMissing){
                while (idx < path.length){
                    ObjectNode newNode = JsonNodeFactory.instance.objectNode();
                    owner.set(path[idx], newNode);
                    owner = newNode;
                    idx++;
                }
                return owner;
            }
            return null;
        }
        JsonNode child = owner.get(name);
        if (idx == path.length - 1)
            return child;

        return child instanceof ObjectNode ?
                findNode((ObjectNode) child, idx + 1, createIfMissing) :
                null;

    }
}

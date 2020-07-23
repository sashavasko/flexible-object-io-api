package org.sv.flexobject.schema;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;

import java.util.HashMap;
import java.util.Map;

public class Schema {

    protected String name;
    protected SchemaElement[] fields;
    protected Map<String, Integer> paramNamesXref = new HashMap<>();

    public Schema(Class<?> dataClass, Enum<?>[] fields) throws NoSuchFieldException {
        this.name = dataClass.getName();
        this.fields = new SchemaElement[fields.length];
        for (Enum<?> e : fields){
            this.fields[e.ordinal()] = new SimpleSchemaElement(dataClass, e);
        }
        initParamXref(dataClass);
    }

    public Schema(Class<?> dataClass, SchemaElement[] fields) throws NoSuchFieldException {
        this.name = dataClass.getName();
        this.fields = fields;
        initParamXref(dataClass);
    }

    public static Schema getRegisteredSchema(Class<?> dataClass){
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName) ?
                SchemaRegistry.getInstance().getSchema(schemaName) : null;
    }

    private void initParamXref(Class<?> dataClass) throws NoSuchFieldException {
        for (SchemaElement f  : fields){
            if (f.getDescriptor() == null) {
                f.setDescriptor(FieldDescriptor.fromEnum(dataClass, (Enum<?>) f));
            }
            paramNamesXref.put(f.getDescriptor().getName(), f.getDescriptor().getOrder()+1);
        }
    }

    public String getName() {
        return name;
    }

    public SchemaElement[] getFields() {
        return fields;
    }

    public FieldDescriptor getFieldDescriptor(Enum<?> e) {
        return fields[e.ordinal()].getDescriptor();
    }

    public Map<String, Integer> getParamNamesXref() {
        return paramNamesXref;
    }

    public void clear(Object datum) throws Exception {
        for (SchemaElement field : fields) {
            field.getDescriptor().clear(datum);
        }
    }

    public boolean load(Object datum, InAdapter input) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().load(datum, input);
        }
        return true;
    }

    public boolean save(Object datum, OutAdapter output) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().save(datum, output);
        }
        output.saveIfYouShould();
        return true;
    }

    public boolean compareFields(Object o1, Object o2){
        try {
            for (SchemaElement field : fields) {
                FieldDescriptor descriptor = field.getDescriptor();
                Object value = descriptor.get(o1);
                if (value != null) {
                    if (!value.equals(descriptor.get(o2)))
                        return false;
                }else if (descriptor.get(o2) != null)
                    return false;
            }
        }catch (Exception e) {
            return false;
        }

        return true;
    }
}

package org.sv.flexobject.schema;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;
import org.sv.flexobject.io.GenericReader;
import org.sv.flexobject.io.GenericWriter;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.io.Writer;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class Schema {

    protected String name;
    private boolean isInferred;
    private SchemaElement[] fields;
    private Map<String, SchemaElement> fieldsByName = new HashMap<>();
    private Map<String, Integer> paramNamesXref = new HashMap<>();
    private Reader reader;
    private Writer writer;

    public Schema(Class<?> dataClass) {
        this.name = dataClass.getName();
        this.isInferred = true;
        Field[] fields = dataClass.getDeclaredFields();
        this.fields = new SchemaElement[fields.length];
        for (int order = 0 ; order < fields.length ; ++order){
            addField(new SimpleSchemaElement(dataClass, fields[order], order), order);
        }
        initParamXref(dataClass);
        SchemaRegistry.getInstance().registerSchema(this);
    }

    public Schema(Class<?> dataClass, Enum<?>[] fields) throws NoSuchFieldException, SchemaException {
        this.name = dataClass.getName();
        this.isInferred = false;
        this.fields = new SchemaElement[fields.length];
        for (Enum<?> e : fields){
            addField(new SimpleSchemaElement(dataClass, e), e.ordinal());
        }
        initParamXref(dataClass);
        SchemaRegistry.getInstance().registerSchema(this);
    }

    public Schema(Class<?> dataClass, SchemaElement[] fields) throws NoSuchFieldException, SchemaException {
        this.name = dataClass.getName();
        this.isInferred = false;
        this.fields = fields;
        for (SchemaElement f  : this.fields) {
            if (f.getDescriptor() == null) {
                f.setDescriptor(FieldDescriptor.fromEnum(dataClass, (Enum<?>) f));
            }
            fieldsByName.put(f.getDescriptor().getName(), f);
        }
        initParamXref(dataClass);
        SchemaRegistry.getInstance().registerSchema(this);
    }

    private void addField(SchemaElement field, int order){
        fields[order] = field;
        fieldsByName.put(field.getDescriptor().getName(), field);
    }

    public static boolean isRegisteredSchema(Class<?> dataClass){
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName);
    }

    public static Schema getRegisteredSchema(Class<?> dataClass){
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName) ?
                SchemaRegistry.getInstance().getSchema(schemaName) : new Schema(dataClass);
    }

    public static Map<String, Integer> getParamNamesXref(Class<?> dataClass){
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().getParamNamesXref(schemaName);
    }

    private void initParamXref(Class<?> dataClass) {
        for (SchemaElement f  : fields){
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

    public FieldDescriptor getDescriptor(String fieldName) {
        return fieldsByName.get(fieldName).getDescriptor();
    }

    public Map<String, Integer> getParamNamesXref() {
        return paramNamesXref;
    }

    public Reader getReader() {
        if (reader == null)
            reader = new GenericReader(this);
        return reader;
    }

    public void setReader(Reader reader) {
        this.reader = reader;
    }

    public Writer getWriter() {
        if (writer == null)
            writer = new GenericWriter(this);
        return writer;
    }

    public void setWriter(Writer writer) {
        this.writer = writer;
    }

    public boolean isInferred() {
        return isInferred;
    }

    public void clear(Object datum) throws Exception {
        for (SchemaElement field : fields) {
            field.getDescriptor().clear(datum);
        }
    }

    public boolean loadFields(Object datum, InAdapter input) throws Exception {
        for (SchemaElement field : fields) {
            field.getDescriptor().load(datum, input);
        }
        return true;
    }

    public Loadable load(Loadable datum, InAdapter input) throws Exception {
        return getReader().convert(input, datum);
    }

    public boolean saveFields(Object datum, OutAdapter output) throws Exception {
        for (SchemaElement field : fields){
            field.getDescriptor().save(datum, output);
        }
        return output.saveIfYouShould();
    }

    public boolean save(Object datum, OutAdapter output) throws Exception {
        return getWriter().convert((Savable) datum, output);
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

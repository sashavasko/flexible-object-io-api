package org.sv.flexobject.schema;

import org.sv.flexobject.*;
import org.sv.flexobject.io.GenericReader;
import org.sv.flexobject.io.GenericWriter;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.io.Writer;
import org.sv.flexobject.schema.annotations.NonStreamableField;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

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
        List<SchemaElement> fieldList = new ArrayList<>();

        addClassFields(dataClass.getSuperclass(), fieldList);
        addClassFields(dataClass, fieldList);

        setFields(fieldList);

        initParamXref(dataClass);
        SchemaRegistry.getInstance().registerSchema(this);
    }

    public static boolean isStreamableField(Field field){
        int mods = field.getModifiers();
        if (Modifier.isStatic(mods) || Modifier.isFinal(mods))
            return false;
        if (field.getAnnotation(NonStreamableField.class) != null)
            return false;
        return true;
    }

    private void addClassFields(Class<?> dataClass, List<SchemaElement> fieldList) {
        if (dataClass != null && !StreamableWithSchema.class.equals(dataClass)) {
            Field[] fields = dataClass.getDeclaredFields();

            for (int order = 0; order < fields.length; ++order) {
                if (isStreamableField(fields[order])) {
                    fieldList.add(new SimpleSchemaElement(dataClass, fields[order], fieldList.size()));
                }
            }
        }
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

    private void setFields(List<SchemaElement> fieldList){
        this.fields = new SchemaElement[fieldList.size()];
        int i = 0;
        for (SchemaElement field : fieldList)
            addField(field, i++);
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

    public static Map<String, Integer> getParamNamesXref(Class<?> dataClass, String ... fields){
        Map<String, Integer> xref = new HashMap<>();
        int fieldIdx = 1;
        for (String field : fields)
            xref.put(field, fieldIdx++);
        return xref;
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

    public void clear(Object datum) throws SchemaException {
        for (SchemaElement field : fields) {
            field.getDescriptor().clear(datum);
        }
    }

    public boolean loadFields(Object datum, InAdapter input) throws SchemaException {
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
                Object otherValue = descriptor.get(o2);
                if (value != null) {
                    if (otherValue == null)
                        return false;

                    if (value.getClass().isArray() != otherValue.getClass().isArray())
                        return false;

                    if (value.getClass().isArray()) {
                        if (!Arrays.equals((Object[])value, (Object[])otherValue)) {
                            return false;
                        }
                    } else if (!value.equals(otherValue))
                        return false;
                } else if (otherValue != null)
                    return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public boolean isEmpty(StreamableWithSchema o) throws SchemaException {
        for (SchemaElement field : fields) {
            FieldDescriptor descriptor = field.getDescriptor();
            if (!descriptor.isEmpty(o))
                return false;
        }
        return true;
    }
}

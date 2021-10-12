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

public class Schema extends AbstractSchema{

    private boolean isInferred;
    private Reader reader;
    private Writer writer;

    public Schema(Class<?> dataClass) {
        super(dataClass);
        this.isInferred = true;
        List<SchemaElement> fieldList = new ArrayList<>();

        addClassFields(dataClass.getSuperclass(), fieldList);
        addClassFields(dataClass, fieldList);

        setFields(fieldList);

        initParamXref();
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
        super(dataClass);
        this.isInferred = false;
        this.fields = new SchemaElement[fields.length];
        for (Enum<?> e : fields){
            addField(new SimpleSchemaElement(dataClass, e), e.ordinal());
        }
        initParamXref();
        SchemaRegistry.getInstance().registerSchema(this);
    }

    public Schema(Class<?> dataClass, SchemaElement[] fields) throws NoSuchFieldException, SchemaException {
        super(dataClass);
        this.isInferred = false;
        this.fields = fields;
        for (SchemaElement f  : this.fields) {
            if (f.getDescriptor() == null) {
                f.setDescriptor(FieldDescriptor.fromEnum(dataClass, (Enum<?>) f));
            }
            fieldsByName.put(f.getDescriptor().getName(), f);
        }
        initParamXref();
        SchemaRegistry.getInstance().registerSchema(this);
    }

    public static boolean isRegisteredSchema(Class<?> dataClass){
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName);
    }

    public static Schema getRegisteredSchema(Class<?> dataClass){
        String schemaName = dataClass.getName();
        if (SchemaRegistry.getInstance().hasSchema(schemaName))
            return SchemaRegistry.getInstance().getSchema(schemaName);

        synchronized (SchemaRegistry.getInstance()) {
            if (SchemaRegistry.getInstance().hasSchema(schemaName))
                return SchemaRegistry.getInstance().getSchema(schemaName);
            return new Schema(dataClass);
        }
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

    public FieldDescriptor getDescriptor(Enum<?> e) {
        return (FieldDescriptor) getFieldDescriptor(e);
    }

    public FieldDescriptor getDescriptor(int i) {
        return (FieldDescriptor) getFieldDescriptor(i);
    }

    public FieldDescriptor getDescriptor(String fieldName) {
        return (FieldDescriptor) getFieldDescriptor(fieldName);
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

    public Loadable load(Loadable datum, InAdapter input) throws Exception {
        return getReader().convert(input, datum);
    }

    public boolean save(Object datum, OutAdapter output) throws Exception {
        return getWriter().convert((Savable) datum, output);
    }
}

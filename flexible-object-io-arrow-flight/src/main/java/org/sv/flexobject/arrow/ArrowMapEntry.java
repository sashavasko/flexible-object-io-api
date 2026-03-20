package org.sv.flexobject.arrow;

import org.sv.flexobject.arrow.read.ArrowMapEntryReader;
import org.sv.flexobject.arrow.read.ArrowStructReader;
import org.sv.flexobject.arrow.write.ArrowMapEntryWriter;
import org.sv.flexobject.arrow.write.ArrowStructWriter;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.StreamableImpl;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Map;

public class ArrowMapEntry extends StreamableImpl {
    private Object key;
    private Object value;

    public ArrowMapEntry() {
    }

    public ArrowMapEntry(Map.Entry entry) {
        this.key = entry.getKey();
        this.value = entry.getValue();
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public static ArrowStructWriter simpleWriter(String fieldName, Field entryField, ValueVector vector) {
        ListVector listVector = (ListVector) vector;
        return new ArrowStructWriter(ArrowMapEntry.class, entryField.getChildren(), fieldName, listVector.getDataVector());
    }

    public static ArrowStructWriter complexWriter(String fieldName, Field entryField, ValueVector vector, Class <? extends Streamable> valueSchema) {
        ListVector listVector = (ListVector) vector;
        return new ArrowMapEntryWriter(entryField.getChildren(), fieldName, listVector.getDataVector(), valueSchema);
    }

    public static ArrowStructReader simpleReader(String fieldName, Field entryField, ValueVector vector) {
        ListVector listVector = (ListVector) vector;
        return new ArrowStructReader(ArrowMapEntry.class, entryField.getChildren(), fieldName, listVector.getDataVector());
    }

    public static ArrowStructReader complexReader(String fieldName, Field entryField, ValueVector vector, Class <? extends Streamable> valueSchema) {
        ListVector listVector = (ListVector) vector;
        return new ArrowMapEntryReader(entryField.getChildren(), fieldName, listVector.getDataVector(), valueSchema);
    }


}

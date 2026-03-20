package org.sv.flexobject.arrow.read;

import org.sv.flexobject.arrow.ArrowMapEntry;
import org.sv.flexobject.Streamable;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapVectorReader extends VectorReader {

    StructListVectorReader entryReader;

    public MapVectorReader(String fieldName, ValueVector vector, Field entry) {
        super(fieldName, vector);
        this.entryReader = new StructListVectorReader(fieldName, vector,
                ArrowMapEntry.simpleReader(fieldName, entry, vector));
    }

    public MapVectorReader(String fieldName, ValueVector vector, Field entry, Class<? extends Streamable> valueSchema) {
        super(fieldName, vector);
        this.entryReader = new StructListVectorReader(fieldName, vector,
                ArrowMapEntry.complexReader(fieldName, entry, vector, valueSchema));
    }

//    @Override
//    public void write(int position, Object value) {
//        if (value == null)
//            entryReader.setNull(position);
//        else
//            entryReader.write(position, ((Map) value).entrySet());
//    }

    @Override
    public boolean isNull(int rowIndex) {
        return entryReader.isNull(rowIndex);
    }

    @Override
    public Object read(int rowIndex) {
        List<ArrowMapEntry> entries = (List<ArrowMapEntry>) entryReader.read(rowIndex);
        Map map = new HashMap();
        for (ArrowMapEntry entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}

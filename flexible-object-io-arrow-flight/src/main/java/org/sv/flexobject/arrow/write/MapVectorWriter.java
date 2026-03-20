package org.sv.flexobject.arrow.write;

import com.carfax.arrow.ArrowMapEntry;
import com.carfax.dt.streaming.Streamable;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.Map;

public class MapVectorWriter extends VectorWriter{

    StructListVectorWriter entryWriter;

    public MapVectorWriter(String fieldName, ValueVector vector, Field entry) {
        super(fieldName, vector);
        this.entryWriter = new StructListVectorWriter(fieldName, vector,
                ArrowMapEntry.simpleWriter(fieldName, entry, vector));
    }

    public MapVectorWriter(String fieldName, ValueVector vector, Field entry, Class<? extends Streamable> valueSchema) {
        super(fieldName, vector);
        this.entryWriter = new StructListVectorWriter(fieldName, vector,
                ArrowMapEntry.complexWriter(fieldName, entry, vector, valueSchema));
    }

    @Override
    public void write(int position, Object value) {
        if (value == null)
            entryWriter.setNull(position);
        else
            entryWriter.write(position, ((Map) value).entrySet());
    }

    @Override
    public void newBatch() {
        entryWriter.newBatch();
    }
}

package org.sv.flexobject.hadoop;


import org.sv.flexobject.avro.AvroSerializationStrategy;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;
import org.sv.flexobject.serde.SerializationStrategy;

import java.io.IOException;

public class TextByteArrayAvro implements StreamableWritable {
    @ScalarFieldTyped(type = DataTypes.binary)
    public byte[] modifiedRawRecord;

    @Override
    public SerializationStrategy getStrategy() throws IOException {
        return AvroSerializationStrategy.AVRO;
    }
}

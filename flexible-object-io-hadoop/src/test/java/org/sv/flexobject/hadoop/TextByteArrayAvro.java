package org.sv.flexobject.hadoop;


import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;

import java.io.IOException;

public class TextByteArrayAvro implements StreamableWritable {
    @ScalarFieldTyped(type = DataTypes.binary)
    public byte[] modifiedRawRecord;

    @Override
    public Strategy getStrategy() throws IOException {
        return Strategy.avro;
    }
}

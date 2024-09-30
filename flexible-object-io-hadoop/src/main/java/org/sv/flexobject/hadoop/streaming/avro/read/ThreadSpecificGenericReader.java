package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;

public class ThreadSpecificGenericReader extends GenericDatumReader {
    public ThreadSpecificGenericReader() {
        super(new GenericData());
    }
}

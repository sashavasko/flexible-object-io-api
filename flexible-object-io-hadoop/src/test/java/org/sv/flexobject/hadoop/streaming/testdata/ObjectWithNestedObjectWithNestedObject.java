package org.sv.flexobject.hadoop.streaming.testdata;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.testdata.levelone.ObjectWithNestedObject;

public class ObjectWithNestedObjectWithNestedObject extends StreamableWithSchema {
    int intField;
    ObjectWithNestedObject objectWithNestedObject;
}

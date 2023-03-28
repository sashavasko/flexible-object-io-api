package org.sv.flexobject.mongo.json.converters;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;
import org.bson.types.ObjectId;

public class HexObjectIdConverter implements Converter<ObjectId> {
    @Override
    public void convert(final ObjectId value, final StrictJsonWriter writer) {
        writer.writeRaw(value.toHexString());
    }
}

package org.sv.flexobject.mongo.json.converters;

import org.bson.json.Converter;
import org.bson.json.StrictJsonWriter;

import java.text.SimpleDateFormat;
import java.util.Date;

import static java.lang.String.format;

public class NormalDateConverter  implements Converter<Long> {
    @Override
    public void convert(final Long value, final StrictJsonWriter writer) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd\'T\'HH:mm:ss.SSSXXX");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        if (value >= -59014396800000L && value <= 253399536000000L) {
            writer.writeRaw(dateFormat.format(new Date(value)));
        } else {
            writer.writeRaw(format("%d", value));
        }
    }
}

package org.sv.flexobject.mongo.schema.testdata;

import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;
import org.sv.flexobject.mongo.schema.annotations.BsonType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public class ObjectWithTimestampAndDate extends SimpleObject<ObjectWithTimestampAndDate> {
    @BsonType(type= org.bson.BsonType.TIMESTAMP)
    public Timestamp timestamp;
    @BsonType(type= org.bson.BsonType.TIMESTAMP)
    public Date timestampFromDate;

    @BsonType(type= org.bson.BsonType.DATE_TIME)
    public Timestamp dateFromTimestamp;
    @BsonType(type= org.bson.BsonType.DATE_TIME)
    public Date date;
    @BsonType(type= org.bson.BsonType.DATE_TIME)
    public LocalDate localDate;

    @Override
    public ObjectWithTimestampAndDate randomInit() {
        super.randomInit();

        timestamp = Timestamp.valueOf(LocalDateTime.now());

        timestamp.setNanos(0);
        timestamp.setTime(timestamp.getTime()/1000l*1000l);
        timestampFromDate = new Date(timestamp.getTime());
        dateFromTimestamp = timestamp;
        date = new Date(timestamp.getTime());
        localDate = LocalDate.now();

        return this;
    }

    public static ObjectWithTimestampAndDate random(){
        ObjectWithTimestampAndDate instance = new ObjectWithTimestampAndDate();
        return instance.randomInit();
    }
}

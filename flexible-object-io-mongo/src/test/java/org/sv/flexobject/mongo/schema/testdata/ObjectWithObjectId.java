package org.sv.flexobject.mongo.schema.testdata;

import org.bson.codecs.pojo.annotations.*;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;
import org.sv.flexobject.mongo.schema.annotations.BsonType;
import org.bson.types.ObjectId;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import java.util.Date;

public class ObjectWithObjectId extends SimpleObject<ObjectWithObjectId> {
    @BsonRepresentation(value = org.bson.BsonType.OBJECT_ID)
    @BsonType(type= org.bson.BsonType.OBJECT_ID)
    public String objectId;

    @Override
    public ObjectWithObjectId randomInit() {
        super.randomInit();
        objectId = new ObjectId(new Date(), (int)Math.round(Math.random() * 16777215)).toHexString();
        System.out.println(objectId);
        return this;
    }

    public static ObjectWithObjectId random(){
        ObjectWithObjectId instance = new ObjectWithObjectId();
        return instance.randomInit();
    }
}

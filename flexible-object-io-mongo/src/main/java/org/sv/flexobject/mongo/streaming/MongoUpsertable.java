package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;

import java.util.List;

public interface MongoUpsertable {
    Bson buildFilter();

    List<Bson> generateUpdates();
    default Bson generateUpdate(){
        return Updates.combine(generateUpdates());
    }

    static List<Bson> updateNullable(List<Bson> updates, String bsonName, Object value){
        if (value != null)
            updates.add(Updates.set(bsonName, value));
        return updates;
    }
}

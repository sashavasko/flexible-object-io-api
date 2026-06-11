package org.sv.flexobject.kafka.streaming;

import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.kafka.KafkaStreamable;
import org.sv.flexobject.schema.DataTypes;

public class KafkaTestData extends StreamableImpl implements KafkaStreamable {
    Long key;
    String value;

    public static KafkaTestData of(long key, String value) {
        KafkaTestData kafkaTestData = new KafkaTestData();
        kafkaTestData.key = key;
        kafkaTestData.value = value;
        return kafkaTestData;
    }

    @Override
    public byte[] getKafkaKey() {
        return DataTypes.longToByte(key);
    }
}

package org.sv.flexobject.hadoop;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Writable;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;
import org.sv.flexobject.hadoop.streaming.avro.read.GenericInputBuilder;
import org.sv.flexobject.hadoop.streaming.avro.write.AvroSink;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface StreamableWritable extends Streamable, Writable {
    enum Strategy{
        json(StreamableWritable::serializeWithJson, StreamableWritable::deserializeWithJson),
        avro(StreamableWritable::serializeWithAvro, StreamableWritable::deserializeWithAvro);

        FunctionWithException<Streamable, byte[], IOException> ser;
        BiConsumerWithException<Streamable, byte[], IOException> deser;

        Strategy(FunctionWithException<Streamable, byte[], IOException> ser, BiConsumerWithException<Streamable, byte[], IOException> deser) {
            this.ser = ser;
            this.deser = deser;
        }
    }

    static byte[] serializeWithJson(Streamable streamable) throws IOException {
        try {
            return streamable.toJsonBytes();
        }catch (IOException ioe){
            throw ioe;
        }catch (Exception e){
            throw new IOException("Failed to serialize object of class " + streamable.getClass().getName(), e);
        }
    }

    static byte[] serializeWithAvro(Streamable streamable) throws IOException {
        AvroSink<Streamable> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(streamable.getClass());
        try {
            sink.put(streamable);
            sink.close();
        }catch (IOException ioe){
            throw ioe;
        }catch (Exception e){
            throw new IOException("Failed to serialize object of class " + streamable.getClass().getName(), e);
        }
        return os.toByteArray();
    }

    static void deserializeWithJson(Streamable streamable, byte[] data) throws IOException {
        try {
            streamable.fromJsonBytes(data);
        } catch (IOException ioe){
            throw ioe;
        } catch (Exception e){
            throw new IOException("Failed to serialize object of class " + streamable.getClass().getName(), e);
        }
    }

    static void deserializeWithAvro(Streamable streamable, byte[] data) throws IOException {
        try(DataFileReader<GenericRecord> reader = GenericInputBuilder.forData(data, streamable.getClass())) {
            GenericRecord record = reader.next();
            AvroSchema.convertGenericRecord(record, record.getSchema(), streamable);
        } catch (IOException ioe){
            throw ioe;
        } catch (Exception e){
            throw new IOException("Failed to serialize object of class " + streamable.getClass().getName(), e);
        }
    }

    default Strategy getStrategy() throws IOException {
        // Avro format is way more expensive for a single record as it also stores schema in it
        return Strategy.json;
    }

    default byte[] toBytes() throws IOException {
        return getStrategy().ser.apply(this);
    }

    default void fromBytes(byte[] data) throws IOException {
        getStrategy().deser.accept(this, data);
    }

    @Override
    default void write(DataOutput out) throws IOException {
        byte[] bytes = toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    default void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        fromBytes(bytes);
    }
}

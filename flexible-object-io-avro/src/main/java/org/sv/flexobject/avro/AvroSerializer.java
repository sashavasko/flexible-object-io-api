package org.sv.flexobject.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.read.StreamableDatumReader;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AvroSerializer {

    Schema schema;
    Class<? extends Streamable> dataClass;

    private static final Map<String, AvroSerializer> instances = new HashMap<>();

    private AvroSerializer(String dataClassName, Class<? extends Streamable> dataClass, Schema schema) {
        if (dataClass == null){
            Class<?> candidate;
            try {
                candidate = DataTypes.classConverter(dataClassName);
            } catch (SchemaException e) {
                throw e;
            } catch (Exception e) {
                throw new SchemaException("Failed to load Avro Data class: " + dataClassName, e);
            }
            if (Streamable.class.isAssignableFrom(candidate)) {
                @SuppressWarnings("unchecked")
                Class<? extends Streamable> accepted = (Class<? extends Streamable>) candidate;
                dataClass = accepted;
            } else
                throw new SchemaException("Unsupported Avro Data class: " + dataClassName + ". Must implement Streamable.");
        }
        this.dataClass = dataClass;
        this.schema = schema == null ? AvroSchema.forClass(dataClass) : schema;
    }

    private static AvroSerializer getInstance(String dataClassName, Class<? extends Streamable> dataClass, Schema schema){
        AvroSerializer instance = instances.get(dataClassName);
        if (instance == null){
            synchronized (instances){
                instance = instances.get(dataClassName);
                if (instance == null){
                    instance = new AvroSerializer(dataClassName, dataClass, schema);
                    instances.put(dataClassName, instance);
                }
            }
        }
        return instance;
    }

    public static AvroSerializer forData(Streamable data){
        return forClass(data.getClass());
    }

    public static AvroSerializer forClass(Class<? extends Streamable> dataClass){
        return getInstance(dataClass.getName(), dataClass, null);
    }

    public static AvroSerializer forClass(String dataClassName){
        return getInstance(dataClassName, null, null);
    }

    public static AvroSerializer forSchema(Schema avroSchema){
        String dataClassName = avroSchema.getFullName();
        SchemaException lastException = null;
        while (dataClassName.contains(".")) {
            try {
                AvroSerializer instance = getInstance(dataClassName, null, avroSchema);
                if (lastException != null) {
                    instances.put(avroSchema.getFullName(), instance);
                }
                return instance;
            }catch (SchemaException e){
                lastException = e;
                int idx = dataClassName.lastIndexOf('.');
                dataClassName = dataClassName.substring(0, idx) + "$" + dataClassName.substring(idx + 1);
            }
        }
        throw lastException == null
                ? new SchemaException("Failed to load Avro Data class: " + avroSchema.getFullName())
                : lastException;
    }

    public static AvroSerializer forClass(Class<? extends Streamable> dataClass, Schema avroSchema){
        return getInstance(dataClass.getName(), dataClass, avroSchema);
    }

    public class WriteOperation {
        NonCopyingByteArrayOutputStream outputStream = new NonCopyingByteArrayOutputStream(1024);
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(outputStream,null);
        GenericDatumWriter<StreamableAvroRecord> writer = new GenericDatumWriter<>();
        StreamableAvroRecord wrapper = new StreamableAvroRecord(schema);

        public WriteOperation() {
            writer.setSchema(schema);
        }

        public WriteOperation write(Streamable data) throws IOException{
            writer.write(wrapper.set(data), encoder);
            return this;
        }

        public ByteBuffer asByteBuffer() throws IOException {
            encoder.flush();
            return outputStream.asByteBuffer();
        }

        public byte[] asBytes() throws IOException {
            encoder.flush();
            return outputStream.toByteArray();
        }

        public WriteOperation write(byte magic) {
            outputStream.write(magic);
            return this;
        }


        public WriteOperation write(int value) {
            outputStream.write((value & 0xFF000000) >> 24);
            outputStream.write((value & 0x0FF0000) >> 16);
            outputStream.write((value & 0x0FF00) >> 8);
            outputStream.write(value);
            return null;
        }
    }

    public class ReadOperation implements Iterable<Streamable> {
        StreamableAvroRecord wrapper = new StreamableAvroRecord(schema);
        StreamableDatumReader reader = new StreamableDatumReader(schema);
        byte[] data;
        int offset;
        int length;

        public ReadOperation() {
        }

        public ReadOperation(byte[] data, int offset, int length) {
            this.data = data;
            this.offset = offset;
            this.length = length;
        }

        protected <T extends Streamable> T read(BinaryDecoder datumIn, T destination) throws IOException {
            @SuppressWarnings("unchecked")
            T data = destination == null ? (T) InstanceFactory.get(dataClass) : destination;
            wrapper.set(data);
            reader.read(wrapper, datumIn);
            return data;
        }

        public ReadOperation setData(byte[] bytes){
            return setData(bytes, 0, bytes.length);
        }

        public ReadOperation setData(byte[] bytes, int offset, int length){
            this.data = bytes;
            this.offset = offset;
            this.length = length;
            return this;
        }

        public <T extends Streamable> T readOne(byte[] bytes) throws IOException {
            return readOne(bytes, 0, bytes.length);
        }

        public <T extends Streamable> T readOne(byte[] bytes, int offset, int length) throws IOException {
            if (bytes == null)
                return null;
            return read(DecoderFactory.get().binaryDecoder(bytes, offset, length, null), null);
        }

        public <T extends Streamable> T readOne(byte[] bytes, T destination) throws IOException {
            return readOne(bytes, 0, bytes.length, destination);
        }

        public <T extends Streamable> T readOne(byte[] bytes, int offset, int length, T destination) throws IOException {
            if (bytes == null)
                return null;
            return read(DecoderFactory.get().binaryDecoder(bytes, offset, length, null), destination);
        }

        public Iterator<? extends Streamable> iterator(byte[] bytes) throws IOException {
            return iterator(bytes, 0, bytes.length);
        }
        public Iterator<? extends Streamable> iterator(byte[] bytes, int offset, int length) throws IOException {
            setData(bytes, offset, length);
            return iterator();
        }

        @Override
        public Iterator<Streamable> iterator() {
            if (data == null)
                return Collections.emptyIterator();

            BinaryDecoder datumIn = DecoderFactory.get().binaryDecoder(data, offset, length, null);

            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    try {
                        return !datumIn.isEnd();
                    } catch (IOException e) {
                        return false;
                    }
                }

                @Override
                public Streamable next() {
                    try {
                        return read(datumIn, null);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }

        public Stream<Streamable> stream() {
            return StreamSupport.stream(spliterator(), false);
        }
    }

    public WriteOperation start(){
        return new WriteOperation();
    }

    public ReadOperation start(byte[] bytes){
        return start(bytes, 0, bytes.length);
    }
    public ReadOperation start(byte[] bytes, int offset, int length){
        return new ReadOperation(bytes, offset, length);
    }

    public ReadOperation startRead(){
        return new ReadOperation();
    }

    public static ByteBuffer toBytes(Streamable data) throws IOException {
        return forData(data).start().write(data).asByteBuffer();
    }

    public static ByteBuffer toBytes(Streamable data, Schema schema) throws IOException {
        return forSchema(schema).start().write(data).asByteBuffer();
    }

    public static <T extends Streamable> T fromBytes(byte[] bytes, Class<? extends Streamable> dataClass) throws Exception {
        return forClass(dataClass).startRead().readOne(bytes);
    }

    public static <T extends Streamable> T fromBytes(byte[] bytes, Class<? extends Streamable> dataClass, Schema avroSchema) throws Exception {
        return forClass(dataClass, avroSchema).startRead().readOne(bytes);
    }
}

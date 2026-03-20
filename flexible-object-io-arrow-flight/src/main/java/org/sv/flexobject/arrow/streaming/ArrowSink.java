package org.sv.flexobject.arrow.streaming;

import org.sv.flexobject.arrow.write.ArrowRootWriter;
import org.sv.flexobject.SaveException;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.util.ConsumerWithException;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public class ArrowSink extends ArrowRootWriter {
    VectorUnloader unloader;
    int batchSize;
    ConsumerWithException<ArrowRecordBatch, Exception> batchConsumer;
    CompressionCodec compressionCodec;

    int batchCount = 0;

    public static class ArrowSinkBuilder<SELF extends ArrowSinkBuilder> extends ArrowRootWriter.Builder<SELF> {

        int batchSize;
        ConsumerWithException<ArrowRecordBatch, Exception> batchConsumer;
        CompressionUtil.CodecType compressionType = CompressionUtil.CodecType.LZ4_FRAME;

        public ArrowSinkBuilder() {
            instanceOf(ArrowSink.class);
        }

        public SELF batchSize(int batchSize) {
            this.batchSize = batchSize;
            return (SELF) this;
        }

        public SELF batchConsumer(ConsumerWithException<ArrowRecordBatch, Exception> batchConsumer) {
            this.batchConsumer = batchConsumer;
            return (SELF) this;
        }

        public SELF compressionType(CompressionUtil.CodecType compressionType) {
            this.compressionType = compressionType;
            return (SELF) this;
        }

        public SELF noCompression() {
            this.compressionType = CompressionUtil.CodecType.NO_COMPRESSION;
            return (SELF) this;
        }

        @Override
        public <O extends ArrowRootWriter> O build() throws ClassNotFoundException, NoSuchFieldException {
            ArrowSink sink = super.build();
            sink.batchSize = batchSize;
            sink.batchConsumer = batchConsumer;
            sink.compressionCodec = CommonsCompressionFactory.INSTANCE.createCodec(compressionType);
            sink.unloader = sink.makeUnloader(sink.getRoot());

            return (O) castToInstance(sink);
        }
    }

    protected VectorUnloader makeUnloader(VectorSchemaRoot root) {
        return new VectorUnloader(root,true, compressionCodec, true);
    }

    public static ArrowSinkBuilder<ArrowSinkBuilder> sinkBuilder() {
        return new ArrowSinkBuilder();
    }

    public ArrowSink() {
    }

    protected VectorUnloader getUnloader() {
        if (unloader == null) {
            unloader = new VectorUnloader(getRoot());
        }
        return unloader;
    }

    @Override
    public boolean put(Streamable value) throws Exception {
        boolean result = super.put(value);
//        System.out.println("Row count : " + getRowCount() + "; batch size : " + batchSize);
        if (result && getRowCount() >= batchSize)
            saveBatch();

        return result;
    }

    @Override
    public void setEOF() {
        try {
            saveBatch();
        } catch (Exception e) {
            throw new SaveException("Failed to save last batch", e);
        }
//        super.setEOF();
    }

    @Override
    public void close() throws Exception {
        setEOF();
        super.close();
    }

    protected void submitBatch() throws Exception {
        ArrowRecordBatch batch = getUnloader().getRecordBatch();
        batchConsumer.accept(batch);
    }

    public void saveBatch() throws Exception {
        if (getRowCount() > 0) {
            commit();

            submitBatch();

            super.newBatch();
            batchCount++;
        }
    }

    public int getBatchCount() {
        return batchCount;
    }
}

package org.sv.flexobject.arrow.streaming;

import com.carfax.arrow.read.ArrowRootReader;
import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.util.SupplierWithException;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.commons.lang3.NotImplementedException;

import java.util.stream.Stream;

public class ArrowSource<T extends Streamable> extends ArrowRootReader {

    SupplierWithException<ArrowRecordBatch, Exception> batchSupplier;
    boolean eof = false;
    VectorLoader loader;

    int batchCount = 0;

    public ArrowSource() {
    }

    public static class ArrowSourceBuilder<SELF extends ArrowSourceBuilder> extends Builder<SELF> {

        SupplierWithException<ArrowRecordBatch, Exception> batchSupplier;

        public ArrowSourceBuilder() {
            instanceOf(ArrowSource.class);
        }

        public SELF supplier(SupplierWithException<ArrowRecordBatch, Exception> supplier){
            this.batchSupplier = supplier;
            return (SELF) this;
        }

        @Override
        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {
            ArrowSource source = super.build();
            source.batchSupplier = this.batchSupplier;
            return (O) castToInstance(source);
        }

    }

    protected VectorLoader makeLoader(VectorSchemaRoot root) {
        return new VectorLoader(root, CommonsCompressionFactory.INSTANCE);
    }

    public static ArrowSourceBuilder<ArrowSourceBuilder> sourceBuilder() {
        return new ArrowSourceBuilder<>();
    }

    @Override
    public boolean hasNext() {
        if (!super.hasNext() && !eof) {
            try {
                if (!getNextBatch()){
                    eof = true;
                    return false;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to load next Arrow batch", e);
            }
        }
        return super.hasNext();
    }

    @Override
    public <O extends Streamable> O get() throws Exception {
        if (!hasNext()) {
            return null;
        }
        return readRecord();
    }

    protected boolean loadNextBatch() throws Exception {
        try(ArrowRecordBatch currentBatch = batchSupplier.get()) {
            if (currentBatch == null)
                return false;

            getLoader().load(currentBatch);
        }
        return true;
    }

    protected boolean getNextBatch() throws Exception {

        if (!loadNextBatch())
            return false;

        reset();
        batchCount++;
//            System.out.println("Loaded a batch of " + currentBatch.getLength() + "; records in root : " + getRoot().getRowCount() + " current row: " + getRowIndex());
        return true;
    }

    private VectorLoader getLoader() {
        if (loader == null)
            loader = makeLoader(getRoot());
        return loader;
    }

    @Override
    public boolean isEOF() {
        return eof;
    }

    @Override
    public Stream<Streamable> stream() {
        throw new NotImplementedException();
    }

    public int getBatchCount() {
        return batchCount;
    }
}

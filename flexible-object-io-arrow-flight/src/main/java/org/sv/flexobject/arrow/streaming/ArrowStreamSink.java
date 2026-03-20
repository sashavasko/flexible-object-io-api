package org.sv.flexobject.arrow.streaming;

import com.carfax.arrow.vector.VectorUtils;
import com.carfax.arrow.write.ArrowRootWriter;
import com.carfax.dt.streaming.SaveException;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compare.VectorEqualsVisitor;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ArrowStreamSink extends ArrowSink {

    public static final Logger logger = LogManager.getLogger(ArrowStreamSink.class);

    // ArrowStreamWriter writer;
    WriteChannel out;
    DictionaryProvider dictionaryProvider;
    IpcOption option;
    Map<Long, FieldVector> previousDictionaries;

    boolean started = false;
    boolean ended = false;
    final Set<Long> dictionaryIdsUsed = new HashSet<>();

    ArrowStreamWriter streamWriter;


    public void batchConsumer(ArrowRecordBatch batch) throws IOException {
        ensureStarted();
        ensureDictionariesWritten();
        writeRecordBatch(batch);
        batch.close();
    }

    public static class ArrowStreamSinkBuilder<SELF extends ArrowStreamSinkBuilder> extends ArrowSinkBuilder<ArrowStreamSinkBuilder> {

        DictionaryProvider dictionaryProvider;
        OutputStream out;
        IpcOption option;

        public ArrowStreamSinkBuilder() {
            instanceOf(ArrowStreamSink.class);
        }

        public SELF dictionaryProvider(DictionaryProvider dictionaryProvider) {
            this.dictionaryProvider = dictionaryProvider;
            return (SELF) this;
        }

        public SELF out(OutputStream out) {
            this.out = out;
            return (SELF) this;
        }

        public SELF option(IpcOption option) {
            this.option = option;
            return (SELF) this;
        }

        public <O extends ArrowRootWriter> O build() throws NoSuchFieldException, ClassNotFoundException {
            ArrowStreamSink sink = super.build();
            sink.batchConsumer = sink::batchConsumer;
            sink.out = new WriteChannel(Channels.newChannel(out));
            sink.dictionaryProvider = dictionaryProvider;
            sink.option = option == null ? IpcOption.DEFAULT : option;

            return (O) castToInstance(sink);
        }
    }

    public static ArrowStreamSinkBuilder<ArrowStreamSinkBuilder> streamSinkBuilder() {
        return new ArrowStreamSinkBuilder();
    }


    protected void ensureDictionariesWritten() throws IOException {

        for (long id : dictionaryIdsUsed) {
            Dictionary dictionary = dictionaryProvider.lookup(id);
            FieldVector vector = dictionary.getVector();
            if (!previousDictionaries.containsKey(id) || !VectorEqualsVisitor.vectorEquals(vector, previousDictionaries.get(id))) {
                writeDictionaryBatch(dictionary);
                if (previousDictionaries.containsKey(id)) {
                    previousDictionaries.get(id).close();
                }

                previousDictionaries.put(id, VectorUtils.copyVector(vector));
            }
        }
    }

    protected void writeDictionaryBatch(Dictionary dictionary) throws IOException {
        FieldVector vector = dictionary.getVector();
        long id = dictionary.getEncoding().getId();
        int count = vector.getValueCount();
        VectorSchemaRoot dictRoot = new VectorSchemaRoot(Collections.singletonList(vector.getField()), Collections.singletonList(vector), count);
        VectorUnloader dictionaryUnloader = makeUnloader(dictRoot);

        try(ArrowDictionaryBatch dictionaryBatch = new ArrowDictionaryBatch(id, dictionaryUnloader.getRecordBatch(), false)) {
            writeDictionaryBatch(dictionaryBatch);
        }
    }

    protected ArrowBlock writeDictionaryBatch(ArrowDictionaryBatch batch) throws IOException {
        ArrowBlock block = MessageSerializer.serialize(out, batch, option);
        if (logger.isDebugEnabled()) {
            logger.debug("DictionaryRecordBatch at {}, metadata: {}, body: {}", new Object[]{block.getOffset(), block.getMetadataLength(), block.getBodyLength()});
        }

        return block;
    }

    protected ArrowBlock writeRecordBatch(ArrowRecordBatch batch) throws IOException {
        ArrowBlock block = MessageSerializer.serialize(out, batch, option);
        if (logger.isDebugEnabled()) {
            logger.debug("RecordBatch at {}, metadata: {}, body: {}", new Object[]{block.getOffset(), block.getMetadataLength(), block.getBodyLength()});
        }

        return block;
    }

    @Override
    public void setEOF() {
        super.setEOF();
        try {
            ensureStarted();
            ensureEnded();
        } catch (IOException e) {
            throw new SaveException("failed to close Arrow stream", e);
        }
    }

    private void ensureStarted() throws IOException {
        if (!started) {
            started = true;
            MessageSerializer.serialize(out, getRoot().getSchema(), option);
        }

    }

    private void ensureEnded() throws IOException {
        if (!ended) {
            ended = true;
            ArrowStreamWriter.writeEndOfStream(out, option);
        }
    }

    public void close() throws Exception {
        setEOF();
        out.close();
        if (previousDictionaries != null)
            AutoCloseables.close(previousDictionaries.values());
        super.close();
    }

}

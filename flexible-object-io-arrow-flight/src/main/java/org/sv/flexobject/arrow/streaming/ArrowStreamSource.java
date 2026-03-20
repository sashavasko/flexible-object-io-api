package org.sv.flexobject.arrow.streaming;

import com.carfax.arrow.read.ArrowRootReader;
import com.carfax.arrow.util.MessageUtils;
import com.carfax.dt.streaming.Streamable;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.*;
import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.VectorBatchAppender;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;

public class ArrowStreamSource<T extends Streamable> extends ArrowSource<T>{
    public static final Logger logger = LogManager.getLogger(ArrowStreamSource.class);

    private MessageChannelReader messageReader;
    int loadedDictionaryCount = 0;
    Schema streamSchema;

    public static class ArrowStreamSourceBuilder<SELF extends ArrowStreamSourceBuilder> extends ArrowSource.Builder<SELF> {

        InputStream in;

        public ArrowStreamSourceBuilder() {
            instanceOf(ArrowStreamSource.class);
        }

        public SELF in(InputStream in) {
            this.in = in;
            return (SELF) this;
        }

        @Override
        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {
            ArrowStreamSource source = super.build();

            ReadableByteChannel channel = Channels.newChannel(in);
            source.messageReader = new MessageChannelReader(new ReadChannel(channel), source.getAllocator());
            source.batchSupplier = source::readNextBatch;

            try {
                source.streamSchema = source.readSchema();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read stream Schema from input", e);
            }

            return (O) castToInstance(source);
        }

    }

    public static ArrowStreamSourceBuilder<ArrowStreamSourceBuilder> streamSourceBuilder() {
        return new ArrowStreamSourceBuilder<>();
    }

    public Schema getStreamSchema() {
        return streamSchema;
    }

    @Override
    public void close() throws Exception {
        super.close();
        AutoCloseables.close(messageReader);
    }

    /**
     * The following borrowed from arrow-vector:ArrowReader
     */


    protected void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
        long id = dictionaryBatch.getDictionaryId();
        Dictionary dictionary = getDictionaryMap().get(id);
        if (dictionary == null) {
            throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
        }
        FieldVector vector = dictionary.getVector();
        // if is deltaVector, concat it with non-delta vector with the same ID.
        if (dictionaryBatch.isDelta()) {
            try (FieldVector deltaVector = vector.getField().createVector(getAllocator())) {
                load(dictionaryBatch, deltaVector);
                VectorBatchAppender.batchAppend(vector, deltaVector);
            }
            return;
        }

        load(dictionaryBatch, vector);
    }

    private void load(ArrowDictionaryBatch dictionaryBatch, FieldVector vector) {
        VectorSchemaRoot root = new VectorSchemaRoot(
                Collections.singletonList(vector.getField()),
                Collections.singletonList(vector), 0);
        VectorLoader loader = makeLoader(root);
        try {
            loader.load(dictionaryBatch.getDictionary());
        } finally {
            dictionaryBatch.close();
        }
    }

    /**
     * The following borrowed from arrow-vector:ArrowStreamReader
     */

    /**
     * Load the next ArrowRecordBatch to the vector schema root if available.
     *
     * @return true if a batch was read, false on EOS
     * @throws IOException on error
     */
    public ArrowRecordBatch readNextBatch() throws IOException {
        logger.debug("Loading next batch...");
        reset();

        MessageResult result = messageReader.readNext();

        // Reached EOS
        if (result == null) {
            logger.debug("result: EndOfStream");
            return null;
        }

        logger.debug("result: headerType = " + MessageUtils.headerType(result.getMessage()));

        if (result.getMessage().headerType() == MessageHeader.RecordBatch) {
            ArrowBuf bodyBuffer = result.getBodyBuffer();

            // For zero-length batches, need an empty buffer to deserialize the batch
            if (bodyBuffer == null) {
                bodyBuffer = getAllocator().getEmpty();
            }

            ArrowRecordBatch batch =
                    MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
            return batch;
        } else if (result.getMessage().headerType() == MessageHeader.DictionaryBatch) {
            // if it's dictionary message, read dictionary message out and continue to read unless get a
            // batch or eos.
            ArrowDictionaryBatch dictionaryBatch = readDictionary(result);
            loadDictionary(dictionaryBatch);
            loadedDictionaryCount++;
            return readNextBatch();
        } else {
            throw new IOException(
                    "Expected RecordBatch or DictionaryBatch but header was "
                            + MessageUtils.headerType(result.getMessage()));
        }
    }

    /** When read a record batch, check whether its dictionaries are available. */
    private void checkDictionaries() throws IOException {
        // if all dictionaries are loaded, return.
        if (loadedDictionaryCount == getDictionaryMap().size()) {
            return;
        }
        for (FieldVector vector : getRoot().getFieldVectors()) {
            DictionaryEncoding encoding = vector.getField().getDictionary();
            if (encoding != null) {
                // if the dictionaries it needs is not available and the vector is not all null, something
                // was wrong.
                if (!getDictionaryMap().containsKey(encoding.getId())
                        && vector.getNullCount() < vector.getValueCount()) {
                    throw new IOException("The dictionary was not available, id was:" + encoding.getId());
                }
            }
        }
    }

    /**
     * Reads the schema message from the beginning of the stream.
     *
     * @return the deserialized arrow schema
     */
    protected Schema readSchema() throws IOException {
        logger.debug("Loading stream Schema ...");
        MessageResult result = messageReader.readNext();

        if (result == null) {
            throw new IOException("Unexpected end of input. Missing schema.");
        }

        if (result.getMessage().headerType() != MessageHeader.Schema) {
            throw new IOException("Expected schema but header was " + result.getMessage().headerType());
        }

        final Schema schema = MessageSerializer.deserializeSchema(result.getMessage());
        MetadataV4UnionChecker.checkRead(
                schema, MetadataVersion.fromFlatbufID(result.getMessage().version()));
        logger.debug("Schema loaded: " + schema);
        return schema;
    }

    private ArrowDictionaryBatch readDictionary(MessageResult result) throws IOException {

        ArrowBuf bodyBuffer = result.getBodyBuffer();

        // For zero-length batches, need an empty buffer to deserialize the batch
        if (bodyBuffer == null) {
            bodyBuffer = getAllocator().getEmpty();
        }

        return MessageSerializer.deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
    }


}

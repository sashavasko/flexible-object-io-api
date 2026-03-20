package org.sv.flexobject.arrow.streaming;

import com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

public class ArrowSourceTest {

    static BufferAllocator rootAllocator;

    @BeforeClass
    public static void beforeClass() throws Exception {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        rootAllocator.close();
    }

    @Test
    public void readWriteSimpleData() throws Exception {
        SimpleObject dataOut1 = SimpleObject.random();
        SimpleObject dataOut2 = SimpleObject.random();
        SimpleObject dataOut3 = SimpleObject.random();
        BlockingQueue<ArrowRecordBatch> queue = new LinkedBlockingQueue<>();

        try(ArrowSink sink = (ArrowSink) ArrowSink.sinkBuilder()
                .batchConsumer(queue::put)
                .batchSize(2)
                .withRootAllocator(rootAllocator)
                .forClass(SimpleObject.class)
                .build()) {

            sink.put(dataOut1);
            sink.put(dataOut2);
            sink.put(dataOut3);
//            sink.commit();
//            System.out.println(ArrowJson.toJsonString(sink.getRoot()));

            sink.setEOF();


            try(ArrowSource<SimpleObject> source = (ArrowSource) ArrowSource.sourceBuilder()
                    .withRootAllocator(rootAllocator)
                    .supplier(queue::poll)
                    .forClass(SimpleObject.class)
                    .build()) {

                SimpleObject dataIn1 = source.get();
                SimpleObject dataIn2 = source.get();
                SimpleObject dataIn3 = source.get();
                SimpleObject dataIn4 = source.get();

                assertEquals(dataOut1, dataIn1);
                assertEquals(dataOut2, dataIn2);
                assertEquals(dataOut3, dataIn3);
                assertNull(dataIn4);

                assertEquals(2, sink.getBatchCount());
                assertEquals(2, source.getBatchCount());
            }
        }
    }
}
package org.sv.flexobject.arrow.streaming;

import org.sv.flexobject.arrow.testdata.SubSchemaInList;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.testdata.TestDataUtils;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ArrowStreamSourceTest {

    static BufferAllocator rootAllocator;

    @BeforeClass
    public static void beforeClass() throws Exception {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        rootAllocator.close();
    }


    public void testSchema(Class<? extends Streamable> dataClass, int dataCount, int batchSize) throws Exception {
        List<Streamable> testData = TestDataUtils.generateTestData(dataClass, dataCount);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try(ArrowStreamSink sink = (ArrowStreamSink) ArrowStreamSink.streamSinkBuilder()
                .out(out)
                .batchSize(batchSize)
                .withRootAllocator(rootAllocator)
                .forClass(dataClass)
                .build()){
            testData.forEach(d-> {
                try {
                    sink.put(d);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        System.out.println("Wrote bytes : " + out.size());

        List<Streamable> actual = new ArrayList<>();

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        try(ArrowStreamSource<SimpleObject> source = (ArrowStreamSource) ArrowStreamSource.streamSourceBuilder()
                .in(in)
                .withRootAllocator(rootAllocator)
                .forClass(dataClass)
                .build()){
            Streamable dataIn;
            do{
                dataIn = source.get();
                if(dataIn == null)
                    break;
                actual.add(dataIn);
            }while (true);
        }

        assertEquals(dataCount, actual.size());
        assertEquals(testData, actual);

    }

    @Test
    public void writeAndReadSimpleObject() throws Exception {
        testSchema(SimpleObject.class, 100, 31);
    }

    @Test
    public void writeAndReadSubSchemaInList() throws Exception {
        testSchema(SubSchemaInList.class, 100, 31);
    }
}
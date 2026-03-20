package org.sv.flexobject.arrow.read;

import com.carfax.arrow.ArrowJson;
import com.carfax.arrow.testdata.IntList;
import com.carfax.arrow.testdata.StringIntMap;
import com.carfax.arrow.testdata.StringObjectMap;
import com.carfax.arrow.testdata.SubSchemaInList;
import com.carfax.arrow.write.ArrowRootWriter;
import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.testdata.TestDataWithSubSchema;
import com.carfax.dt.streaming.testdata.TestDataWithSubSchemaInCollection;
import com.carfax.dt.streaming.testdata.levelone.ObjectWithNestedObject;
import com.carfax.dt.streaming.testdata.levelone.leveltwo.SimpleObject;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class ArrowRootReaderTest {

    public void testRead(Class <? extends Streamable> schema, VectorSchemaRoot root, Streamable dataOut1, Streamable dataOut2) throws NoSuchFieldException, ClassNotFoundException {
        ArrowRootReader reader = ArrowRootReader.builder().forClass(schema).withRoot(root).build();

        Streamable dataIn1 = reader.readRecord();
        Streamable dataIn2 = reader.readRecord();
        System.out.println(dataIn1);
        System.out.println(dataIn2);
        assertEquals(dataOut1, dataIn1);
        assertEquals(dataOut2, dataIn2);

    }

    public void testCase(Class <? extends Streamable> schema) throws Exception {
        Method random = null;
        Method randomB = null;
        try {
            random = schema.getMethod("random");
        } catch (NoSuchMethodException e) {
            randomB = schema.getMethod("random", boolean.class);
        }
        VectorSchemaRoot root;
        Streamable dataOut1 = (Streamable) (random == null ? randomB.invoke(null, true): random.invoke(null));
        Streamable dataOut2 = (Streamable) (random == null ? randomB.invoke(null, false): random.invoke(null));
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(schema).build()){
            writer.writeRecord(dataOut1);
            writer.writeRecord(dataOut2);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));
            System.out.println(dataOut1);
            System.out.println(dataOut2);

            testRead(schema, root, dataOut1, dataOut2);
        }
    }


    @Test
    public void simpleObject() throws Exception {
        testCase(SimpleObject.class);
    }

    @Test
    public void nestedObject() throws Exception {
        testCase(ObjectWithNestedObject.class);
    }


    @Test
    public void intList() throws Exception {
        testCase(IntList.class);
    }

    @Test
    public void subSchemaList() throws Exception {
        testCase(SubSchemaInList.class);
    }

    @Test
    public void stringIntMap() throws Exception {
        testCase(StringIntMap.class);
    }


    @Test
    public void stringObjectMap() throws Exception {
        testCase(StringObjectMap.class);
    }

    @Test
    public void testDataWithSubSchemaInCollection() throws Exception {
        testCase(TestDataWithSubSchemaInCollection.class);
    }

    @Test
    public void testDataWithSubSchema() throws Exception {
        testCase(TestDataWithSubSchema.class);
    }

}
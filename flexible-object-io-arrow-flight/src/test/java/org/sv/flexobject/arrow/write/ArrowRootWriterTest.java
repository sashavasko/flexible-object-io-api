package org.sv.flexobject.arrow.write;

import org.sv.flexobject.arrow.ArrowJson;
import org.sv.flexobject.arrow.testdata.IntList;
import org.sv.flexobject.arrow.testdata.StringIntMap;
import org.sv.flexobject.arrow.testdata.StringObjectMap;
import org.sv.flexobject.arrow.testdata.SubSchemaInList;
import org.sv.flexobject.testdata.levelone.ObjectWithNestedObject;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.reader.BaseReader;
import org.junit.Test;

import static org.junit.Assert.*;

public class ArrowRootWriterTest {

    @Test
    public void simpleObject() throws Exception {
        VectorSchemaRoot root;
        SimpleObject simpleObject = SimpleObject.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(SimpleObject.class).build()){
            writer.writeRecord(simpleObject);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));

            IntVector intVector = (IntVector) root.getVector("intField");
            assertEquals(1, intVector.getValueCount());
            assertEquals(simpleObject.intField, (int)intVector.getObject(0));

        }
    }

    @Test
    public void nestedObject() throws Exception {
        VectorSchemaRoot root;
        ObjectWithNestedObject dataOut = ObjectWithNestedObject.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(ObjectWithNestedObject.class).build()){
            writer.writeRecord(dataOut);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));

            IntVector intVector = (IntVector) root.getVector("intField");

            assertEquals(1, intVector.getValueCount());
            assertEquals(dataOut.intField, (int)intVector.getObject(0));

            StructVector nestedObjectVector = (StructVector) root.getVector("nestedObject");

            IntVector nestedIntVector = (IntVector) nestedObjectVector.getChild("intField");
            assertNotNull(nestedIntVector);
            assertNotNull(nestedIntVector.getObject(0));
            assertEquals(dataOut.nestedObject.intField, (int)nestedIntVector.getObject(0));

        }
    }

    @Test
    public void intList() throws Exception {
        VectorSchemaRoot root;
        IntList dataOut = IntList.random();
        IntList dataOut2 = IntList.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(IntList.class).build()){
            writer.writeRecord(dataOut);
            writer.writeRecord(dataOut2);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));
            System.out.println(dataOut);
            System.out.println(dataOut2);

            VarCharVector varCharVector = (VarCharVector) root.getVector("stringField");

            assertEquals(2, varCharVector.getValueCount());
            assertEquals(dataOut.stringField, varCharVector.getObject(0).toString());
            assertEquals(dataOut2.stringField, varCharVector.getObject(1).toString());

            ListVector listVector = (ListVector) root.getVector("intList");
            assertNotNull(listVector);
            assertEquals(2, listVector.getValueCount());
            FieldVector listDataVector = listVector.getDataVector();
            assertEquals(8, listDataVector.getValueCount());
            assertEquals(dataOut.intList.get(0), listDataVector.getObject(0));
            assertEquals(dataOut.intList.get(1), listDataVector.getObject(1));
            assertEquals(dataOut.intList.get(2), listDataVector.getObject(2));
            assertEquals(dataOut.intList.get(3), listDataVector.getObject(3));
            assertEquals(dataOut2.intList.get(0), listDataVector.getObject(4));
            assertEquals(dataOut2.intList.get(1), listDataVector.getObject(5));
            assertEquals(dataOut2.intList.get(2), listDataVector.getObject(6));
            assertEquals(dataOut2.intList.get(3), listDataVector.getObject(7));
        }
    }

    @Test
    public void subSchemaList() throws Exception {
        VectorSchemaRoot root;
        SubSchemaInList dataOut = SubSchemaInList.random();
        SubSchemaInList dataOut2 = SubSchemaInList.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(SubSchemaInList.class).build()){
            writer.writeRecord(dataOut);
            writer.writeRecord(dataOut2);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));
            System.out.println(dataOut);
            System.out.println(dataOut2);

            VarCharVector varCharVector = (VarCharVector) root.getVector("stringField");

            assertEquals(2, varCharVector.getValueCount());
            assertEquals(dataOut.stringField, varCharVector.getObject(0).toString());
            assertEquals(dataOut2.stringField, varCharVector.getObject(1).toString());

            ListVector listVector = (ListVector) root.getVector("listOfObjects");
            assertNotNull(listVector);
            assertEquals(2, listVector.getValueCount());

            FieldVector fieldVector = listVector.getDataVector();
            assertNotNull(fieldVector);
            assertTrue(fieldVector instanceof StructVector);

            StructVector structVector = (StructVector) fieldVector;

            IntVector nestedIntVector = (IntVector) structVector.getChild("intField");
            assertNotNull(nestedIntVector);
            assertEquals(6, nestedIntVector.getValueCount());
            assertEquals(dataOut.listOfObjects.get(0).intField, (int)nestedIntVector.getObject(0));
            assertEquals(dataOut.listOfObjects.get(1).intField, (int)nestedIntVector.getObject(1));
            assertEquals(dataOut.listOfObjects.get(2).intField, (int)nestedIntVector.getObject(2));
            assertEquals(dataOut2.listOfObjects.get(0).intField, (int)nestedIntVector.getObject(3));
            assertEquals(dataOut2.listOfObjects.get(1).intField, (int)nestedIntVector.getObject(4));
            assertEquals(dataOut2.listOfObjects.get(2).intField, (int)nestedIntVector.getObject(5));
        }
    }

    @Test
    public void stringIntMap() throws Exception {
        VectorSchemaRoot root;
        StringIntMap dataOut = StringIntMap.random();
        StringIntMap dataOut2 = StringIntMap.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(StringIntMap.class).build()){
            writer.writeRecord(dataOut);
            writer.writeRecord(dataOut2);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));
            System.out.println(dataOut);
            System.out.println(dataOut2);

            VarCharVector varCharVector = (VarCharVector) root.getVector("stringField");

            assertEquals(2, varCharVector.getValueCount());
            assertEquals(dataOut.stringField, varCharVector.getObject(0).toString());
            assertEquals(dataOut2.stringField, varCharVector.getObject(1).toString());

            MapVector mapVector = (MapVector) root.getVector("map");
            assertNotNull(mapVector);
            assertEquals(2, mapVector.getValueCount());

            UnionMapReader reader = mapVector.getReader();

            int recordsLeft = dataOut.map.size();
            StringIntMap currentData = dataOut;
            while(reader.next()) {
                String key = reader.key().readText().toString();
                assertEquals(currentData.map.get(key), reader.key().readInteger());
                if (--recordsLeft == 0) {
                    currentData = dataOut2;
                }
            }
        }
    }

    @Test
    public void stringObjectMap() throws Exception {
        VectorSchemaRoot root;
        StringObjectMap dataOut = StringObjectMap.random();
        StringObjectMap dataOut2 = StringObjectMap.random();
        try(ArrowRootWriter writer = ArrowRootWriter.builder().forClass(StringObjectMap.class).build()){
            writer.writeRecord(dataOut);
            writer.writeRecord(dataOut2);
            writer.commit();

            root = writer.getRoot();
            System.out.println(ArrowJson.toJsonString(root));
            System.out.println(dataOut);
            System.out.println(dataOut2);

            VarCharVector varCharVector = (VarCharVector) root.getVector("stringField");

            assertEquals(2, varCharVector.getValueCount());
            assertEquals(dataOut.stringField, varCharVector.getObject(0).toString());
            assertEquals(dataOut2.stringField, varCharVector.getObject(1).toString());

            MapVector mapVector = (MapVector) root.getVector("map");
            assertNotNull(mapVector);
            assertEquals(2, mapVector.getValueCount());

            UnionMapReader reader = mapVector.getReader();
            reader.setPosition(0);

            int recordsLeft = dataOut.map.size();
            StringObjectMap currentData = dataOut;
            while(reader.next()) {
                String key = reader.key().readText().toString();
                BaseReader.StructReader valueReader = reader.value();
                if (currentData.map.get(key) == null){
                    assertNull(valueReader.reader("intField").readInteger());
                } else {
                    int valueIntValue = valueReader.reader("intField").readInteger();
                    assertEquals(currentData.map.get(key).intField, valueIntValue);
                }

                if (--recordsLeft == 0) {
                    currentData = dataOut2;
                }
            }
        }
    }
}
package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public class SchemaTest {

    public static class TestData extends StreamableImpl{
        int foo;
    }

    public static class TestDataSetOfLongs extends StreamableImpl{
        @ValueType(type = DataTypes.int64)
        Set<Long> setOfLongs = new HashSet<>();
        public void addValues(long ... values){
            for (long v : values)
                setOfLongs.add(v);
        }
    }

    //    @Test
//    public void constructor() {
//        Type superclass = getClass().getGenericSuperclass();
//        Type type = ((ParameterizedType) superclass).getActualTypeArguments()[0];
//        Constructor<TestData> constructor;
//        Class<?> rawType = type instanceof Class<?>
//                ? (Class<?>) type
//                : (Class<?>) ((ParameterizedType) type).getRawType();
//        constructor = rawType.getConstructor();
//    }
//
    @Test
    public void getParamNamesXref() {
        assertNotNull(Schema.getParamNamesXref(TestData.class));
    }

    @Test
    public void toFromJson() throws Exception {
        TestDataSetOfLongs testData = new TestDataSetOfLongs();
        testData.addValues(1234567l, 9876543l);
        JsonNode json = testData.toJson();
        TestDataSetOfLongs loaded = new TestDataSetOfLongs();
        loaded.fromJson(json);
        System.out.println(loaded);
        assertTrue(loaded.setOfLongs.contains(1234567l));

        assertEquals(testData, loaded);
    }

}
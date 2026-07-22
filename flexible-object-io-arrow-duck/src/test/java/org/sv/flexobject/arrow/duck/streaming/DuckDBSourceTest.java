package org.sv.flexobject.arrow.duck.streaming;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.properties.FilePropertiesProvider;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;

import java.util.ArrayList;
import java.util.List;

class DuckDBSourceTest {


//    @Disabled
//    @Test
//    void generateFile() throws Exception {
//        List<Streamable> testData = TestDataUtils.generateTestData(TestDataWithSubSchemaInCollection.class, 100);
//        try(ParquetSink sink = ParquetSink.builder()
//                .forOutput("src/test/resources/db/data/testDB.parquet")
//                .withSchema(TestDataWithSubSchemaInCollection.class)
//                .build()){
//            for (Streamable testDatum : testData) {
//                sink.put(testDatum);
//            }
//        }
//    }

    @Test
    void sampleDb() throws Exception {
        ConnectionManager.getInstance()
                .registerPropertiesProvider(new FilePropertiesProvider("src/test/resources/db"))
                .registerProvider(DuckDBConnectionProvider.class);
        List<Streamable> recordsOut = new ArrayList<>();
        try(DuckDBSource<TestDataWithSubSchemaInCollection> source = DuckDBSource.sourceBuilder()
                .connection("testDuck")
                .query("SELECT * FROM testDB where intField = 813777806")
                .forClass(TestDataWithSubSchemaInCollection.class)
                .build()){
            for (Streamable data : source){
                recordsOut.add(data);
                System.out.println(data);
            }
        }
    }
}
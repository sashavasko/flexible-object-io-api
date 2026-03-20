package org.sv.flexobject.arrow.store;

import org.sv.flexobject.arrow.ArrowFlightConf;
import org.sv.flexobject.arrow.read.ArrowRecordReader;
import org.sv.flexobject.arrow.read.ArrowRootReader;
import org.sv.flexobject.arrow.streaming.FlightEndpointSource;
import org.sv.flexobject.arrow.streaming.FlightSink;
import org.sv.flexobject.arrow.streaming.FlightSource;
import org.sv.flexobject.arrow.testdata.SubSchemaInList;
import org.sv.flexobject.arrow.write.ArrowRootWriter;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.testdata.TestDataUtils;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;

public class MemoryStoreServerTest {

    static BufferAllocator rootAllocator;
    FlightDescriptor testFlightDescriptor = FlightDescriptor.path("foobar");

    @BeforeClass
    public static void beforeClass() throws Exception {
        rootAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        rootAllocator.close();
    }

    @Test
    public void testSimpleExchange() throws Exception {
        testShemaExchange(SubSchemaInList.class);
    }

    public void testShemaExchange(Class <? extends Streamable> schema) throws Exception {
        try (MemoryStoreServer server = new MemoryStoreServer(ArrowFlightConf.forLocalTest())){
            server.init(rootAllocator);
            Thread serverThread = new Thread(server);
            serverThread.start();

            Collection<Streamable> dataOut1 = generateTestData(schema);
            Collection<Streamable> dataOut2 = generateTestData(schema);
            try(FlightClient client = server.getArrowConf().createClient(rootAllocator)) {
                storeDataFlightSink(client, schema, dataOut1, dataOut2);
//                storeDataPrimitive(client, schema, dataOut1, dataOut2);

                List<Streamable> dataIn = retrieveDataFlightSource(client, schema);
//                List<Streamable> dataIn = retrieveDataFlightEndpointSource(client, schema);
                List<Streamable> dataOut = new ArrayList<>();
                dataOut.addAll(dataOut1);
                dataOut.addAll(dataOut2);
                assertEquals(dataOut, dataIn);

//                List<List<Streamable>> dataIn = retrieveDataPrimitive(client, schema);
//                assertEquals(dataOut1, dataIn.get(0));
//                assertEquals(dataOut2, dataIn.get(1));
            }
        }
    }

    private List<Streamable> retrieveDataFlightEndpointSource(FlightClient client, Class<? extends Streamable> schema) throws Exception {
        FlightInfo info = client.getInfo(testFlightDescriptor);
        List<FlightEndpoint> endpoints = info.getEndpoints();
        List<Streamable> dataIn = new ArrayList<>();
        if (endpoints.isEmpty()) {
            throw new RuntimeException("No endpoints returned from Flight server.");
        }

        for (FlightEndpoint endpoint : endpoints) {
            // 3. Download the data from the server.
            try(FlightEndpointSource source = (FlightEndpointSource) FlightEndpointSource.flightEndpointSourceBuilder()
                    .usingClient(client)
                    .endpoint(endpoint)
                    .forClass(schema)
                    .build()) {

                while (source.hasNext()) {
                    dataIn.add(source.get());
                }
            }
        }
        return dataIn;
    }

    private List<Streamable> retrieveDataFlightSource(FlightClient client, Class<? extends Streamable> schema) throws Exception {
        List<Streamable> dataIn = new ArrayList<>();
        try(FlightSource source = (FlightSource) FlightSource.builder()
                .descriptor(testFlightDescriptor)
                .usingClient(client)
                .forClass(schema)
                .buildSource()) {

            while (source.hasNext()) {
                dataIn.add(source.get());
            }
        }
        return dataIn;
    }

    private List<List<Streamable>> retrieveDataPrimitive(FlightClient client, Class<? extends Streamable> schema) throws Exception {
        FlightInfo info = client.getInfo(testFlightDescriptor);
        List<FlightEndpoint> endpoints = info.getEndpoints();
        List<List<Streamable>> dataIn = new ArrayList<>();
        if (endpoints.isEmpty()) {
            throw new RuntimeException("No endpoints returned from Flight server.");
        }

        for (FlightEndpoint endpoint : endpoints) {
            // 3. Download the data from the server.
            try(FlightStream stream = client.getStream(endpoint.getTicket())) {
                VectorSchemaRoot streamRoot = stream.getRoot();

                while (stream.next()) {
                    ArrowRecordReader reader = ArrowRootReader.builder().withRoot(streamRoot).forClass(schema).build();
                    Streamable record;
                    List<Streamable> batch = new ArrayList<>();
                    while ((record = reader.readRecord()) != null) {
                        batch.add(record);
                    }
                    dataIn.add(batch);
                }
            }
        }
        return dataIn;
    }
//    private Collection<Streamable> retrieveData(FlightClient client, Class<? extends Streamable> schema) throws NoSuchFieldException, ClassNotFoundException {
//        FlightInfo info = client.getInfo(testFlightDescriptor);
//        List<FlightEndpoint> endpoints = info.getEndpoints();
//        List<Streamable> dataIn = new ArrayList<>();
//        if (endpoints.isEmpty()) {
//            throw new RuntimeException("No endpoints returned from Flight server.");
//        }
//
//        for (FlightEndpoint endpoint : endpoints) {
//            // 3. Download the data from the server.
//            FlightStream stream = client.getStream(endpoint.getTicket());
//            VectorSchemaRoot streamRoot = stream.getRoot();
//            VectorSchemaRoot downloadedRoot = VectorSchemaRoot.create(streamRoot.getSchema(), rootAllocator);
//            VectorLoader loader = new VectorLoader(downloadedRoot);
//            VectorUnloader unloader = new VectorUnloader(streamRoot);
//
//            while (stream.next()) {
//                try (final ArrowRecordBatch arb = unloader.getRecordBatch()) {
//                    loader.load(arb);
//                    ArrowRecordReader reader = ArrowRootReader.builder().withRoot(downloadedRoot).forClass(schema).build();
//                    Streamable record;
//                    while ((record = reader.readRecord()) != null) {
//                        dataIn.add(record);
//                    }
//                }
//            }
//        }
//        return dataIn;
//    }

    private void storeDataFlightSink(FlightClient client, Class<? extends Streamable> schema, Collection<Streamable> ... dataOut) throws Exception {
        try(FlightSink sink = (FlightSink) FlightSink.flightSinkBuilder()
                .usingClient(client)
                .descriptor(testFlightDescriptor)
                .batchSize(2)
                .withRootAllocator(rootAllocator)
                .forClass(schema)
                .build()){
            for (Collection<Streamable> batch : dataOut) {
                for (Streamable streamable : batch) {
                    sink.put(streamable);
                }
            }
        }
    }

    private void storeDataPrimitive(FlightClient client, Class<? extends Streamable> schema, Collection<Streamable> ... dataOut) throws Exception {
        try(ArrowRootWriter writer = ArrowRootWriter.builder().withRootAllocator(rootAllocator).forClass(schema).build()) {
            FlightClient.ClientStreamListener stream = client.startPut(testFlightDescriptor, writer.getRoot(), new AsyncPutListener());
            for (Collection<Streamable> batch : dataOut) {
                writer.newBatch();
                for (Streamable streamable : batch) {
                    writer.writeRecord(streamable);
                }
                writer.commit();
                stream.putNext();
            }
            stream.completed();
            stream.getResult();
        }
    }

    private List<Streamable> generateTestData(Class<? extends Streamable> schema) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        return TestDataUtils.generateTestData(schema, 2);
    }
}
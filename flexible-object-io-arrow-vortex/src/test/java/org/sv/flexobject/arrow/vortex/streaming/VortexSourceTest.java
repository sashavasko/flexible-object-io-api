package org.sv.flexobject.arrow.vortex.streaming;

import dev.vortex.api.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class VortexSourceTest {
    @Test
    void readSampleFile() throws IOException {
        Session session = Session.create();
        Map<String, String> properties = new HashMap<>();
        /**
         * For remote storage, the map contains the credentials/options for the underlying object store. For example, for S3 you'd expect things like:
         *
         * aws_access_key_id
         * aws_secret_access_key
         * aws_region
         * endpoint/credential options
         */

        DataSource vortexDataSource = DataSource.open(session, "file:///tmp/data.vortex", properties);
        ScanOptions scanOptions = ScanOptions.builder().build();
        Scan scan = vortexDataSource.scan(scanOptions);
        BufferAllocator allocator = new RootAllocator();
        while(scan.hasNext()){
            Partition partition = scan.next();
            ArrowReader arrowReader = partition.scanArrow(allocator);
            arrowReader.loadNextBatch();
            VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();
            Map<Long, Dictionary> dictionaryMap = arrowReader.getDictionaryVectors();

        }
    }
}
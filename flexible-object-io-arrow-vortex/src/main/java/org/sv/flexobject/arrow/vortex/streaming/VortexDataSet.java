package org.sv.flexobject.arrow.vortex.streaming;

import dev.vortex.api.DataSource;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;

import java.util.Map;

public class VortexDataSet {

    DataSource dataSource;

    public VortexDataSet(String url) {
        dataSource = DataSource.open(VortexSession.get(), url);
    }

    public VortexDataSet(String url, Map<String, String> properties) {
        dataSource = DataSource.open(VortexSession.get(), url, properties);
    }

    public VortexPartitionSet startScan(ScanOptions scanOptions){
        Scan scan = dataSource.scan(scanOptions);
        return new VortexPartitionSet(scan);
    }
}

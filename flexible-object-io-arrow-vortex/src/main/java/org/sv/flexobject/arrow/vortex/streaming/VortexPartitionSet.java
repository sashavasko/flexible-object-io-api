package org.sv.flexobject.arrow.vortex.streaming;

import dev.vortex.api.Partition;
import dev.vortex.api.Scan;

import java.util.Iterator;

public class VortexPartitionSet implements Iterable<Partition> {
    Scan scan;

    public VortexPartitionSet(Scan scan) {
        this.scan = scan;
    }

    @Override
    public Iterator<Partition> iterator() {
        return scan;
    }
}

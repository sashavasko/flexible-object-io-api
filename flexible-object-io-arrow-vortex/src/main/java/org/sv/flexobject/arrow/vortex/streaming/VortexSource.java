package org.sv.flexobject.arrow.vortex.streaming;

import dev.vortex.api.Partition;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.arrow.read.ArrowRootReader;
import org.sv.flexobject.util.AutoCloseables;

import java.io.IOException;

public class VortexSource  <T extends Streamable> extends ArrowRootReader {

    Partition partition;
    ArrowReader arrowReader;

    public static class VortexSourceBuilder extends Builder<VortexSourceBuilder> {
        Partition partition;

        public VortexSourceBuilder() {
            instanceOf(VortexSource.class);
        }

        @Override
        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {
            VortexSource source = new VortexSource();
            source.partition = this.partition;
            source.setAllocator(getAllocator());
            return (O) castToInstance(source);
        }
    }

    public static VortexSourceBuilder sourceBuilder(){
        return new VortexSourceBuilder();
    }

    @Override
    public boolean hasNext() {
        if (arrowReader == null){
            arrowReader = partition.scanArrow(getAllocator());
        } else if (super.hasNext())
            return true;

        try {
            if (arrowReader.loadNextBatch()) {
                reset();
                return true;
            }
        } catch (IOException e) {
            return false;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        AutoCloseables.close(arrowReader);
        // All of the Vortex gets closed using Java's Cleaner. Just gotta make sure we don't reference them any more.
        partition = null;
    }
}


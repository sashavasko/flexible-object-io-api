package org.sv.flexobject.arrow.streaming;

import org.sv.flexobject.arrow.write.ArrowRootWriter;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;

public class FlightSink extends ArrowSink {

    FlightClient.ClientStreamListener stream;

    public static class FlightSinkBuilder<SELF extends FlightSinkBuilder> extends ArrowSinkBuilder<SELF> {
        FlightClient client;
        FlightDescriptor descriptor;
        FlightClient.PutListener metadataListener;

        public FlightSinkBuilder() {
            instanceOf(FlightSink.class);
        }

        public SELF usingClient(FlightClient client) {
            this.client = client;
            return (SELF) this;
        }

        public SELF descriptor(FlightDescriptor descriptor) {
            this.descriptor = descriptor;
            return (SELF) this;
        }

        public SELF metadataListener(FlightClient.PutListener metadataListener) {
            this.metadataListener = metadataListener;
            return (SELF) this;
        }

        public <O extends ArrowRootWriter> O build() throws ClassNotFoundException, NoSuchFieldException {
            FlightSink sink = super.build();

            if (metadataListener == null)
                metadataListener = new AsyncPutListener();

            sink.stream = client.startPut(descriptor, sink.getRoot(), metadataListener);

            return (O) castToInstance(sink);
        }

    }

    public static FlightSinkBuilder<FlightSinkBuilder> flightSinkBuilder(){
        return new FlightSinkBuilder();
    }

    @Override
    protected void submitBatch() throws Exception {
        stream.putNext();
    }

    @Override
    public void setEOF() {
        super.setEOF();
        stream.completed();
        stream.getResult();
    }
}

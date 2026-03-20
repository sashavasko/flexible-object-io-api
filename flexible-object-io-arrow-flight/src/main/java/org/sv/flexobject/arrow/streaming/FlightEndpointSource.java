package org.sv.flexobject.arrow.streaming;

import org.sv.flexobject.arrow.read.ArrowRootReader;
import org.sv.flexobject.Streamable;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightStream;

public class FlightEndpointSource<T extends Streamable> extends ArrowSource<T> {
    FlightStream stream;

    public static class FlightEndpointSourceBuilder<SELF extends FlightEndpointSourceBuilder> extends ArrowSource.ArrowSourceBuilder<SELF> {
        FlightClient client;
        FlightEndpoint endpoint;

        public FlightEndpointSourceBuilder() {
            instanceOf(FlightEndpointSource.class);
        }

        public SELF usingClient(FlightClient client) {
            this.client = client;
            return (SELF) this;
        }

        public SELF endpoint(FlightEndpoint endpoint) {
            this.endpoint = endpoint;
            return (SELF) this;
        }

        @Override
        public <O extends ArrowRootReader> O build() throws ClassNotFoundException, NoSuchFieldException {
            FlightStream stream = client.getStream(endpoint.getTicket());
            withRoot(stream.getRoot());

            FlightEndpointSource source = super.build();

            source.stream = stream;
            return (O) castToInstance(source);
        }

    }

    public static FlightEndpointSourceBuilder<FlightEndpointSourceBuilder> flightEndpointSourceBuilder(){
        return new FlightEndpointSourceBuilder();
    }

    @Override
    protected boolean loadNextBatch() throws Exception {
        if (!stream.next()){
            return false;
        }
        System.out.println("Loaded batch...");


        return true;
    }

    @Override
    public void close() throws Exception {
        super.close();
        stream.close();
    }
}

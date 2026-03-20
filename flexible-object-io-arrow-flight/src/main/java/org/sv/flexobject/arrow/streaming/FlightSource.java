package org.sv.flexobject.arrow.streaming;

import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.stream.Source;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.util.AutoCloseables;
import org.apache.commons.lang3.NotImplementedException;

import java.util.stream.Stream;

public class FlightSource<T extends Streamable> implements Source<T> {

    FlightEndpointSource.FlightEndpointSourceBuilder endpointSourceBuilder;
    FlightInfo info;

    int currentEndpointIdx = 0;
    FlightEndpointSource<T> currentSource;

    public static class FlightSourceBuilder<SELF extends FlightSourceBuilder> extends FlightEndpointSource.FlightEndpointSourceBuilder<SELF> {

        FlightDescriptor descriptor;
        CallOptions callOptions;

        public SELF descriptor(FlightDescriptor descriptor) {
            this.descriptor = descriptor;
            return (SELF) this;
        }

        @Override
        public <O extends Source> O buildSource() {
            FlightSource source = new FlightSource();

            source.endpointSourceBuilder = this;
            source.info = client.getInfo(descriptor);
            if (source.info.getEndpoints().isEmpty()) {
                throw new RuntimeException("No endpoints returned from Flight server.");
            }

            return (O) source;
        }
    }

    public static FlightSourceBuilder<FlightSourceBuilder> builder(){
        return new FlightSourceBuilder();
    }

    @Override
    public <O extends T> O get() throws Exception {
        if (ensureValidEndpoit())
            return currentSource.get();

        return null;
    }

    @Override
    public boolean isEOF() {
        return !ensureValidEndpoit();
    }

    public boolean hasNext(){
        if (!ensureValidEndpoit())
            return false;
        return currentSource.hasNext();
    }

    private boolean ensureValidEndpoit() {
        if (currentSource != null && !currentSource.isEOF())
            return true;
        if (info.getEndpoints().size() <= currentEndpointIdx)
            return false;

        try {
            currentSource = (FlightEndpointSource<T>) endpointSourceBuilder.endpoint(info.getEndpoints().get(currentEndpointIdx++)).build();
        } catch (Exception e) {
            return false;
        }

        return !currentSource.isEOF();
    }

    @Override
    public void close() throws Exception {
        AutoCloseables.close(currentSource );
        currentEndpointIdx = info.getEndpoints().size();
        Source.super.close();
    }

    @Override
    public Stream<T> stream() {
        throw new NotImplementedException();
    }
}

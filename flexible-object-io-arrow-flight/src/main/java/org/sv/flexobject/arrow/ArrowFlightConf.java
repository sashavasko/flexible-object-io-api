package org.sv.flexobject.arrow;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapperBase;
import org.sv.flexobject.utility.InstanceFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.File;
import java.io.IOException;

public class ArrowFlightConf extends HadoopPropertiesWrapperBase<ArrowFlightConf> {

    String scheme;
    String hostname;
    int flightPort;
    String username;

    Class<? extends BufferAllocator> allocatorClass;
    boolean useTls;
    String certChainPath;
    String certKeyPath;

    @Override
    public ArrowFlightConf setDefaults() {
        scheme = "https";
//        hostname = "dremiod02p.d.carfax.us";
        flightPort = 32010;
        allocatorClass = RootAllocator.class;
        useTls = false;
        return null;
    }

    public static ArrowFlightConf forLocalTest(){
        ArrowFlightConf conf = new ArrowFlightConf();
        conf.hostname = "localhost";
        conf.scheme = "http";
        return conf;
    }

    public String getScheme() {
        return scheme;
    }

    public String getHostname() {
        return hostname;
    }

    public int getFlightPort() {
        return flightPort;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setScheme(String scheme) {
        this.scheme = scheme;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public BufferAllocator getAllocator() {
        return InstanceFactory.get(allocatorClass);
    }

    public Location getLocation() {
        return  useTls ?
                Location.forGrpcTls(hostname, flightPort) :
                Location.forGrpcInsecure(hostname, flightPort);
    }

    public Flight.Location getFlightLocation() {
        return  Flight.Location.newBuilder().setUri(scheme + "://" + hostname + ":" + flightPort).build();
    }

    public FlightServer.Builder getServerBuilder(FlightProducer producer) throws IOException {
        BufferAllocator allocator = getAllocator();
        FlightServer.Builder builder =  FlightServer.builder(
                allocator,
                getLocation(),
                producer
        );
        if (useTls)
            builder.useTls(new File(certChainPath), new File(certKeyPath));

        return builder;
    }

    public FlightClient.Builder getClientBuilder() {
        BufferAllocator allocator = getAllocator();
        FlightClient.Builder builder = FlightClient.builder(
                allocator,
                getLocation()
        );

        return builder;
    }

    public FlightClient createClient(BufferAllocator rootAllocator) throws IOException {
        return getClientBuilder().allocator(rootAllocator.newChildAllocator("flight-client", 0, Long.MAX_VALUE)).build();
    }

    public FlightClient createClient() throws IOException {
        return getClientBuilder().build();
    }

}

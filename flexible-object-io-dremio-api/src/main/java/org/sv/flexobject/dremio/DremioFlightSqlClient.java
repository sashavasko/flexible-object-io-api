package org.sv.flexobject.dremio;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.memory.BufferAllocator;

public class DremioFlightSqlClient extends FlightSqlClient {
    DremioClientConf conf;
    FlightClient flightClient;
    BufferAllocator allocator;


    public DremioFlightSqlClient(DremioClientConf conf, FlightClient flightClient, BufferAllocator allocator) {
        super(flightClient);
        this.conf = conf;
        this.flightClient = flightClient;
        this.allocator = allocator;
    }

    public FlightClient getFlightClient() {
        return flightClient;
    }

    public DremioClientConf getConf() {
        return conf;
    }

    @Override
    public void close() throws Exception {
        super.close();
        allocator.close();
    }
}

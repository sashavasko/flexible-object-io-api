package org.sv.flexobject.dremio;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Arrays;
import java.util.Properties;

public class DremioFlightSqlClientProvider  implements ConnectionProvider {

    public static final Logger logger = LogManager.getLogger(DremioFlightSqlClientProvider.class);

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        logger.debug("### Dremio Flight props : " + connectionProperties.toString());
        DremioClientConf conf = InstanceFactory.get(DremioClientConf.class).from(connectionProperties);
        logger.info("### Dremio Flight client config :" + conf.toString());

        final Location clientLocation = Location.forGrpcInsecure(conf.getHostname(), conf.getFlightPort());
        final BufferAllocator allocator = conf.getAllocator();

        FlightClient flightClient = FlightClient.builder(allocator, clientLocation).build();
        String userName = conf.getUsername();
        String password = secret.toString();
        System.out.println("\n\n\n############################################u: " + userName + " p: " + password);
        flightClient.authenticateBasic(conf.getUsername(), secret.toString());
        return new DremioFlightSqlClient(conf, flightClient, allocator);
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(DremioFlightSqlClient.class);
    }
}

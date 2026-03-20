package org.sv.flexobject.arrow;

import com.carfax.dt.streaming.connections.ConnectionProvider;
import com.carfax.utility.InstanceFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Properties;

public class FlightClientProvider implements ConnectionProvider {

    public static final Logger logger = LogManager.getLogger(FlightClientProvider.class);

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {
        logger.debug("### Flight props : " + connectionProperties.toString());
        ArrowFlightConf conf = InstanceFactory.get(ArrowFlightConf.class).from(connectionProperties);
        logger.info("### Flight client config :" + conf.toString());

        FlightClient flightClient = conf.getClientBuilder().build();
        String userName = conf.getUsername();
        String password = secret.toString();
        if (StringUtils.isNotEmpty(userName) && StringUtils.isNotEmpty(password)) {
            logger.debug("\n############################################u: " + userName + " p: " + password);
            flightClient.authenticateBasic(conf.getUsername(), secret.toString());
        }
        return flightClient;
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(FlightClient.class);
    }
}

package org.sv.flexobject.dremio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Arrays;
import java.util.Properties;

public class DremioClientProvider implements ConnectionProvider {

    public static final Logger logger = LogManager.getLogger(DremioClientProvider.class);

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) throws Exception {

        logger.debug("### Dremio props : " + connectionProperties.toString());
        DremioClientConf conf = InstanceFactory.get(DremioClientConf.class).from(connectionProperties);
        logger.info("### Dremio client config :" + conf.toString());


        return DremioClient.builder().forConf(conf).withPassword(secret).build();
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(DremioClient.class);
    }
}

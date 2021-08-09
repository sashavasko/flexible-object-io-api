package org.sv.flexobject.clustering;

import org.junit.Test;
import org.sv.flexobject.properties.Namespace;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProbabilityBasedThrottlingStrategyTest {

    @Test
    public void testConfiguration() throws Exception {
        Namespace parent = new Namespace(Namespace.getDefaultNamespace(), "cluster");
        ProbabilityBasedThrottlingStrategy.Configuration configuration = new ProbabilityBasedThrottlingStrategy.Configuration(parent);
        Map map = configuration.getMap(HashMap.class);

        System.out.println(map);
        assertEquals(13000l, map.get("sv.cluster.strategy.master.concerning.response.micros"));

        map.put("sv.cluster.strategy.master.concerning.response.micros", 25000l);

        configuration.from(map);

        assertEquals(25000l, configuration.masterConcerningResponseMicros);

        map.clear();
        map.put("sv.cluster.strategy.master.concerning.response.micros", 35000l);
        configuration.from(map);

        assertEquals(35000l, configuration.masterConcerningResponseMicros);
        assertEquals(60000l, configuration.firstAvailableSlaveThresholdMicros);
    }

    @Test
    public void masterOverloadedResponseMicros() throws Exception {
        ProbabilityBasedThrottlingStrategy strategy = new ProbabilityBasedThrottlingStrategy();

        strategy.getConfiguration().set("masterConcerningResponseMicros", 30000l);
        strategy.getConfiguration().set("masterOverloadedResponseMicros", 45000l);

        assertEquals(45000l, strategy.config.masterOverloadedResponseMicros);
    }
}
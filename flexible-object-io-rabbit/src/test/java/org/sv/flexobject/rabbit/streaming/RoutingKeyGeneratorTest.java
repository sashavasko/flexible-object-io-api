package org.sv.flexobject.rabbit.streaming;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RoutingKeyGeneratorTest {

    @Test
    public void constantStringAlwaysReturnsConfiguredKey() {
        RoutingKeyGenerator.ConstantString generator = new RoutingKeyGenerator.ConstantString("routing.key");

        assertEquals("routing.key", generator.makeKey("message"));
    }

    @Test
    public void randomNumberReturnsValueWithinConfiguredLimit() {
        RoutingKeyGenerator.RandomNumber generator = new RoutingKeyGenerator.RandomNumber(10);

        int value = Integer.parseInt(generator.makeKey("message"));

        assertTrue(value >= 0);
        assertTrue(value < 10);
    }
}

package org.sv.flexobject.rabbit.streaming;

import org.apache.commons.lang3.RandomUtils;

public interface RoutingKeyGenerator<MESSAGE_TYPE> {

    String makeKey(MESSAGE_TYPE message);

    class ConstantString implements RoutingKeyGenerator{

        String routingKey;

        public ConstantString(String routingKey) {
            this.routingKey = routingKey;
        }

        @Override
        public String makeKey(Object message) {
            return routingKey;
        }
    }

    class RandomNumber implements RoutingKeyGenerator{

        int randomLimit;

        public RandomNumber(int randomLimit) {
            this.randomLimit = randomLimit;
        }

        @Override
        public String makeKey(Object message) {
            return String.valueOf(RandomUtils.nextInt(0, randomLimit));
        }
    }
}

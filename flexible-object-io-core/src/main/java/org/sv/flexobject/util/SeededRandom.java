package org.sv.flexobject.util;

import java.util.Random;

public class SeededRandom extends Random {
    public SeededRandom() {
        super(System.nanoTime());
    }
}

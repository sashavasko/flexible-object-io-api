package org.sv.flexobject.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class StringIdCalculatorTest {

    @Test
    public void test_19UDE2F76KA002630() {
        StringIdCalculator calculator = new StringIdCalculator();

        assertEquals(7914066825060038149L, (long)calculator.calculate("19UDE2F76KA002630"));
        assertEquals(3685274545L, (long)calculator.getCrc32c());
        assertEquals(3429381637L, (long)calculator.getBase64crc32c());
    }

}
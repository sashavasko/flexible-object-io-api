package org.sv.flexobject.testdata;


import org.sv.flexobject.Streamable;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

public class TestDataUtils {

    public static List<Streamable> generateTestData(Class<? extends Streamable> dataClass, int count) {
        Method random = null;
        Method randomB = null;
        try {
            random = dataClass.getMethod("random");
        } catch (NoSuchMethodException e) {
            try {
                randomB = dataClass.getMethod("random", boolean.class);
            } catch (NoSuchMethodException ex) {
                throw new RuntimeException("Cannot generate test data: no 'random()' nor 'random(boolean)' method defined for " + dataClass.getName());
            }
        }

        List<Streamable> data = new ArrayList<>();
        for (int i = 0; i < count; i++) {

            Streamable instance = null;
            try {
                instance = (Streamable) (random == null ? randomB.invoke(null, (i%2) == 0): random.invoke(null));
            } catch (Exception e) {
                throw new RuntimeException("Failed to generate test data for class " + dataClass.getName(), e);
            }
            data.add(instance);
        }
        return data;
    }
}

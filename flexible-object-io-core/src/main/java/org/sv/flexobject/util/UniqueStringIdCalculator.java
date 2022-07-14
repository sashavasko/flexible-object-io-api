package org.sv.flexobject.util;

import java.util.HashMap;
import java.util.Map;

public class UniqueStringIdCalculator extends StringIdCalculator{
    Map<String, Long> exceptions;
    long nextExceptionId = MIN_ID-1;

    public Map<String, Long> getExceptions() {
        return exceptions;
    }

    public void setExceptions(Map<String, Long> exceptions) {
        this.exceptions = exceptions;
        nextExceptionId = MIN_ID-1;
        for (Long id : exceptions.values()){
            if (nextExceptionId >= id)
                nextExceptionId = id-1;
        }
    }

    public long addException(String s){
        if (exceptions == null)
            exceptions = new HashMap<>();
        if (exceptions.containsKey(s)){
            return exceptions.get(s);
        }
        exceptions.put(s, nextExceptionId);
        nextExceptionId--;
        return nextExceptionId + 1;
    }

    @Override
    public long calculate(String s) {
        if (exceptions != null && exceptions.containsKey(s))
            return exceptions.get(s);
        return super.calculate(s);
    }
}

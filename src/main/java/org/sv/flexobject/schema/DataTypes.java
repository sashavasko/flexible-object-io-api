package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.util.BiFunctionWithException;
import org.sv.flexobject.util.TriConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public enum DataTypes {
    string( (adapter,fieldName)->{return adapter.getString(fieldName);},
            (adapter,fieldName,value)->{adapter.setString(fieldName, (String) value);}),
    jsonNode( (adapter,fieldName)->{return adapter.getJson(fieldName);},
            (adapter,fieldName,value)->{adapter.setJson(fieldName, (JsonNode) value);}),
    int32( (adapter,fieldName)->{return adapter.getInt(fieldName);},
            (adapter,fieldName,value)->{adapter.setInt(fieldName, (Integer) value);}),
    bool( (adapter,fieldName)->{return adapter.getBoolean(fieldName);},
            (adapter,fieldName,value)->{adapter.setBoolean(fieldName, (Boolean) value);}),
    int64( (adapter,fieldName)->{return adapter.getLong(fieldName);},
            (adapter,fieldName,value)->{adapter.setLong(fieldName, (Long) value);}),
    date( (adapter,fieldName)->{return adapter.getDate(fieldName);},
            (adapter,fieldName,value)->{adapter.setDate(fieldName, (Date) value);}),
    timestamp( (adapter,fieldName)->{return adapter.getTimestamp(fieldName);},
            (adapter,fieldName,value)->{adapter.setTimestamp(fieldName, (Timestamp) value);}),
    localDate( (adapter,fieldName)->{return adapter.getLocalDate(fieldName);},
            (adapter,fieldName,value)->{adapter.setDate(fieldName, (LocalDate) value);}); //   LocalDate getLocalDate (String fieldName) throws Exception

    protected BiFunctionWithException<InAdapter, String, Object, Exception> getter;
    protected TriConsumerWithException<OutAdapter, String, Object, Exception> setter;

    DataTypes(BiFunctionWithException<InAdapter, String, Object, Exception> getter, TriConsumerWithException<OutAdapter, String, Object, Exception> setter) {
        this.getter = getter;
        this.setter = setter;
    }

    public Object get(InAdapter adapter, String fieldName) throws Exception {
        return getter.apply(adapter, fieldName);
    }

    public Object get(InAdapter adapter, String fieldName, Object defaultValue) throws Exception {
        Object result = getter.apply(adapter, fieldName);
        return result == null ? defaultValue: result;
    }

    public void set(OutAdapter adapter, String fieldName, Object value) throws Exception {
        setter.accept(adapter, fieldName, value);
    }
}

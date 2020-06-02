package org.sv.flexobject.adapter;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;

public interface AdapterFactory {

    InAdapter createInputAdapter(String id, Object... parameters);
    OutAdapter createOutputAdapter(String id, Object... parameters);
}

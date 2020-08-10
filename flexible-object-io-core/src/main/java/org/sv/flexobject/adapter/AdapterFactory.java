package org.sv.flexobject.adapter;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;

public interface AdapterFactory {

    InAdapter createInputAdapter(String id);
    OutAdapter createOutputAdapter(String id);
}

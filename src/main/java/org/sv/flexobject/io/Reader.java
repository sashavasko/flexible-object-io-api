package org.sv.flexobject.io;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;

public interface Reader {

    Loadable create() throws Exception;

    default Loadable convert(InAdapter input, Loadable datum) throws Exception{
        return datum.load(input) ? datum : null;
    }

    default Loadable convert(InAdapter input) throws Exception {
        Loadable datum = create();
        return convert(input, datum);
    }

}

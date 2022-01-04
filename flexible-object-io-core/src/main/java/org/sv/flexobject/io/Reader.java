package org.sv.flexobject.io;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;

public interface Reader<T extends Loadable> {

    T create() throws Exception;

    default T convert(InAdapter input, T datum) throws Exception{
        return datum.load(input) ? datum : null;
    }

    default T convert(InAdapter input) throws Exception {
        T datum = create();
        return convert(input, datum);
    }

}

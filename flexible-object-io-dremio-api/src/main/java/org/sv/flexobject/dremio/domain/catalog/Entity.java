package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;

import java.util.List;

public class Entity extends ApiData {
    public EntityType entityType;  //
    public String id;          // UUID
    public String name;
    /***
     *  Unique identifier of the version of the source that you want to update.
     *  Dremio uses the tag to ensure that you are requesting to update the most
     *  recent version of the source.
     *  Example:   T0/Zr1FOY3A=
     */
    public String tag;


    public List<String> getPath(){
        throw new UnsupportedOperationException("The entity " + name + " and id " + id + " does not have Path");
    }
}

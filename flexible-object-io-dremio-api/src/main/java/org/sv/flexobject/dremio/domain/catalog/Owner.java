package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;

public class Owner extends StreamableImpl {
    public String ownerId; // owner UUID
    public OwnerType ownerType;

}

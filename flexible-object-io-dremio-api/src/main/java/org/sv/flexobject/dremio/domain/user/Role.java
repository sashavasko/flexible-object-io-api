package org.sv.flexobject.dremio.domain.user;

import org.sv.flexobject.dremio.domain.ApiData;

public class Role extends ApiData {
    public String id; // UUID
    public String name;
    public RoleType type;

}

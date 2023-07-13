package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.ApiData;
import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class Grant extends ApiData {
    // cannot use enum for some reason due to peculiarities
    // of server-side Dremio API implementation
    @ValueType(type = DataTypes.string)
    private List<String> privileges = new ArrayList<>();
    public GranteeType granteeType;
    public String id; //UUID, Unique identifier of the user or role.
    public String name;
    public String firstName;
    public String lastName;
    public String email;

    public EnumSet<Permissions> getPrivileges() {
        EnumSet<Permissions> permissionsEnumSet = EnumSet.noneOf(Permissions.class);
        for (String permission : privileges){
            permissionsEnumSet.add(Permissions.valueOf(permission));
        }
        return permissionsEnumSet;
    }

    public void setPrivileges(EnumSet<Permissions>privileges) {
        this.privileges.clear();
        for (Permissions permission : privileges){
            this.privileges.add(permission.name());
        }
    }
}

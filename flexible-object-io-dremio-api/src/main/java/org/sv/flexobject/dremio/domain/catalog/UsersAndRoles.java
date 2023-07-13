package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.EnumSetField;

import java.util.EnumSet;

// https://docs.dremio.com/software/rest-api/catalog/container-source/#users-and-roles-2

public class UsersAndRoles extends StreamableImpl {
    public String id; // Unique identifier of the user or role who should have access to the source.
    @EnumSetField(enumClass = Permissions.class, emptyValue = "null")
    public EnumSet<Permissions> permissions = EnumSet.noneOf(Permissions.class); // List of privileges the user or role should have on the source
}

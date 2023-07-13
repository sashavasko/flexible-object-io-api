package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.List;

// Information about users and roles that should have access to the source and
// the specific privileges each user or role should have.
// May include an array of users, an array of roles, or both,
// depending on the configured access and privileges.
public class AccessControlList extends StreamableImpl {
    @ValueClass(valueClass = UsersAndRoles.class)
    public List<UsersAndRoles> users = new ArrayList<>();
    @ValueClass(valueClass = UsersAndRoles.class)
    public List<UsersAndRoles> roles = new ArrayList<>();
}

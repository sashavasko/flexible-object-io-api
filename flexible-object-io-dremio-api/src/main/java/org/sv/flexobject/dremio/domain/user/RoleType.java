package org.sv.flexobject.dremio.domain.user;

public enum RoleType {
    INTERNAL,   //: Role was created in the Dremio user interface (UI) or with the Role API.
    EXTERNAL,   //: Role was imported from an external service like Microsoft Azure Active Directory, Lightweight Directory Access Protocol (LDAP), or a System for Cross-domain Identity Management (SCIM) provider.
    SYSTEM      //: Role was predefined in Dremio.

}

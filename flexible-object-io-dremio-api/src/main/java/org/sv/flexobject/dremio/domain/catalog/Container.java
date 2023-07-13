package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.*;

public class Container extends Entity{
    @ValueClass(valueClass = Child.class)
    public List<Child> children = new ArrayList<>();

    /***
     * To keep existing accessControlList settings while making other updates,
     * duplicate the existing accessControlList object in the PUT request.
     */
    public AccessControlList accessControlList;
    /***
     * List of the privileges that you have on the source.
     * Empty unless the request URL includes the permissions query parameter
     */
    @EnumSetField(enumClass = Permissions.class,emptyValue = "null")
    public Set<Permissions> permissions = EnumSet.noneOf(Permissions.class);
    public Owner owner;

    public Optional<Child> findChildByName(String childName){
        return children.stream()
                .filter(c->c.path.get(c.path.size()-1).equals(childName))
                .findFirst();
    }

}

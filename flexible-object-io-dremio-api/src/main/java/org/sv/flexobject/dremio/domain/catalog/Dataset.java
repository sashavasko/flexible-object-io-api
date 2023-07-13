package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.dremio.domain.Permissions;
import org.sv.flexobject.dremio.domain.schema.Field;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.EnumSetField;
import org.sv.flexobject.schema.annotations.ValueClass;
import org.sv.flexobject.schema.annotations.ValueType;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class Dataset extends Entity{
    public TableType type;
    @ValueType(type = DataTypes.string)
    public List<String> path = new ArrayList<>();

    public Timestamp createdAt;
    public AccessControlList accessControlList;
    @EnumSetField(enumClass = Permissions.class,emptyValue = "null")
    public Set<Permissions> permissions = EnumSet.noneOf(Permissions.class);

    public Owner owner;

    @ValueClass(valueClass = Field.class)
    public List<Field> fields = new ArrayList<>();

    public Dataset() {
        entityType = EntityType.dataset;
    }

    @Override
    public List<String> getPath() {
        return path;
    }

    public String getName(){
        return path.get(path.size()-1);
    }

    public List<String> getContext(){
        List<String> context = new ArrayList<>(path);
        context.remove(context.size()-1);
        return context;
    }
}

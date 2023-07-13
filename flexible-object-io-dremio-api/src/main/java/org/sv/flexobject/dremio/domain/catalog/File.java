package org.sv.flexobject.dremio.domain.catalog;

import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class File extends Entity {
    /**
     * Path of the file within Dremio, expressed as an array.
     * The path consists of the source or space,
     * followed by any folder and subfolders,
     * followed by the target file itself as the last item in the array.
     */
    @ValueType(type = DataTypes.string)
    public List<String> path = new ArrayList<>();

    public File() {
        entityType = EntityType.file;
    }

    @Override
    public List<String> getPath() {
        return path;
    }
}

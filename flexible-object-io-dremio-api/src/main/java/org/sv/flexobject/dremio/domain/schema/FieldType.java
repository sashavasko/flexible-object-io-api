package org.sv.flexobject.dremio.domain.schema;

import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.annotations.ValueClass;

import java.util.ArrayList;
import java.util.List;

public class FieldType extends StreamableImpl {
    public String name; // cannot use Enum FieldTypeNames because of the stupid types with spaces in the name
    public Integer precision; // Total number of digits in the number. Included only for the DECIMAL type.
    public Integer scale; // Number of digits to the right of the decimal point. Included only for the DECIMAL type
    @ValueClass(valueClass = SubSchema.class)
    public List<SubSchema> subSchema = new ArrayList<>();

}

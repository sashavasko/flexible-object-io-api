package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class Child extends StreamableImpl {
    public String id; //uuid
    /***
     * Path to the child catalog object within the source, expressed as an array.
     * The path consists of the source, followed by the name of the folder, file,
     * or dataset itself as the last item in the array.
     * Example
     * [ "AWS-S3_testgroup", "archive.dremio.com" ]
     */
    @ValueType(type = DataTypes.string)
    public List<String> path = new ArrayList<>();

    public String tag;
    public CatalogType type;
    public ContainerType containerType; // For catalog objects with the type CONTAINER, the containerType is FOLDER

    /***
     * For catalog objects with the type DATASET, the type of dataset.
     * If the dataset is from an external source such as PostgreSQL,
     * the datasetType is DIRECT.
     * For tables, the datasetType is PROMOTED.
     * For views, the datasetType is VIRTUAL
     */
    public DatasetType datasetType;
}

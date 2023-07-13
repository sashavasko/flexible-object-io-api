package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.List;

public class Format extends StreamableImpl {
    public FormatType type;

    /**
     * Table name. Dremio automatically duplicates the name of the origin file or folder to populate this value.
     */
    public String name;

    /**
     * Path of the table within Dremio, expressed as an array.
     * The path consists of the source or space,
     * followed by any folder and subfolders,
     * followed by the table itself as the last item in the array.
     * Example
     * [ "Samples", "samples.dremio.com", "Dremio University", "restaurant_reviews.parquet" ]
     */
    @ValueType(type = DataTypes.string)
    public List<String> fullPath = new ArrayList<>();

    public Integer ctime; // Not used. Has the value 0.
    public Boolean isFolder;

    /**
     * Location, expressed as a string, where the table's metadata
     * is stored within a Dremio source or space. Example :
     * /samples.dremio.com/Dremio University/restaurant_reviews.parquet
     */
    public String location;

    public String metaStoreType; // Not used. Has the value HDFS.
    public ParquetDataFormat parquetDataFormat;

    // List of data format types in the table. Included only for Iceberg tables, and PARQUET is the only valid valu
    @ValueType(type = DataTypes.string)
    public List<String> dataFormatTypeList = new ArrayList<>();

    public String sheetName;   // For tables created from files that contain multiple sheets, the name of the sheet used to create the table
    public Boolean extractHeader; // For tables created from files, the value is true if Dremio extracted the table's column names from the first line of the file.
    public Boolean hasMergedCells; // For tables created from files, the value is true if Dremio expanded merged cells in the file when creating the table.
    public String fieldDelimiter; // Character used to indicate separate fields in the table. May be , for a comma (default), \t for a tab, | for a pipe, or a custom character.
    public String quote; //Character used for quotation marks in the table. May be \" for a double quote (default), ' for a single quote, or a custom character
    public String comment; //Character used to indicate comments in the table. May be # for a number sign (default) or a custom character.
    public String escape; //Character used to indicate an escape in the table. May be " for a double quote (default), ` for a back quote, \\ for a backward slash, or a custom character.
    public String lineDelimiter; //Character used to indicate separate lines in the table. May be \r\n for a carriage return and a new line (default), \n for a new line, or a custom character.
    public Boolean skipFirstLine;//If Dremio skipped the first line in the file or folder when creating the table, the value is true. Otherwise, the value is false.
    public Boolean autoGenerateColumnNames; //If Dremio used the existing columnn names in the file or folder for the table columns, the value is true. Otherwise, the value is false.
    public Boolean trimHeader; //If Dremio trimmed column names to a specific number of characters when creating the table, the value is true. Otherwise, the value is false.
    public Boolean autoCorrectCorruptDates;

    public Format() {
    }

    public Format(FormatType type) {
        this.type = type;
    }

    public Format dontAutoCorrectCorruptDates(){
        autoCorrectCorruptDates = false;
        return this;
    }
}

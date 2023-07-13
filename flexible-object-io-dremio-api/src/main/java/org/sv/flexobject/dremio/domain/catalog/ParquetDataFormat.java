package org.sv.flexobject.dremio.domain.catalog;


import org.sv.flexobject.StreamableImpl;

public class ParquetDataFormat extends StreamableImpl {
    public FormatType type = FormatType.Parquet;
    public Integer ctime; // Not used. Has the value 0
    public Boolean isFolder;
    public Boolean autoCorrectCorruptDates; // If the value is true, Dremio automatically corrects corrupted date fields in the table. Otherwise, the value is false.
}

package org.sv.flexobject.dremio.domain.catalog;

public enum FormatType {
    Delta,
    Excel,
    Iceberg,
    JSON,
    Parquet,
    Text,
    Unknown,
    XLS;

    public static final Format PARQUET = new Format(Parquet).dontAutoCorrectCorruptDates();
    public static final Format TEXT = new Format(Text);
    public static final Format UNKNOWN = new Format(Unknown);
    public static final Format JSON_FORMAT = new Format(JSON);
    public static final Format XLS_FORMAT = new Format(XLS);
    public static final Format ICEBERG = new Format(Iceberg);
    public static final Format EXCEL = new Format(Excel);
    public static final Format DELTA = new Format(Delta);

}

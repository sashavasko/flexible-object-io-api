package org.sv.flexobject.dremio.domain.schema;

public enum FieldTypeName {
    STRUCT,
    LIST,
    UNION,
    INTEGER,
    BIGINT,
    FLOAT,
    DOUBLE,
    VARCHAR,
    VARBINARY,
    BOOLEAN,
    DECIMAL,
    TIME,
    DATE,
    TIMESTAMP,
    INTERVAL_DAY_TO_SECOND,
    INTERVAL_YEAR_TO_MONTH;


    @Override
    public String toString() {
        return super.toString().replace("_", " ");
    }
}

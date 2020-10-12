package org.sv.flexobject.hadoop.streaming.parquet.write;

public class ParquetWriteException extends Exception {
    String fieldName;

    public ParquetWriteException(String fieldName) {
        this.fieldName = fieldName;
    }

    public ParquetWriteException(String message, String fieldName) {
        super(message);
        this.fieldName = fieldName;
    }

    public ParquetWriteException(String message, Throwable cause, String fieldName) {
        super(message, cause);
        this.fieldName = fieldName;
    }

    public String formatOptionalData(){
        return null;
    }

    @Override
    public String toString() {
        String optionalData = formatOptionalData();

        return getClass().getSimpleName() + ": " + getLocalizedMessage() +
                " {fieldName='" + fieldName
                + (optionalData == null ? "'}" : "', " + optionalData + '}');
    }
}

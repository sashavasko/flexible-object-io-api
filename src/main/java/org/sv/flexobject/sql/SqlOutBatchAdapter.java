package org.sv.flexobject.sql;

import java.sql.PreparedStatement;
import java.util.Map;

public class SqlOutBatchAdapter extends SqlOutAdapter implements AutoCloseable {

    public enum PARAMS {
        batchSize
    }

    protected long recordsAdded = 0;
    protected long recordsExecuted = 0;
    protected long batchSize = 1000;

    public SqlOutBatchAdapter() {
    }

    public SqlOutBatchAdapter(Map<String, Integer> paramNamesXref, long batchSize) {
        super(paramNamesXref);
        this.batchSize = batchSize;
    }

    public SqlOutBatchAdapter(PreparedStatement st, Map<String, Integer> paramNamesXref, long batchSize) {
        super(st, paramNamesXref);
        this.batchSize = batchSize;
    }

    @Override
    public SqlOutAdapter save() throws Exception {
        preparedStatement.addBatch();
        ++recordsAdded;
        if (recordsAdded  >= recordsExecuted + batchSize) {
            preparedStatement.executeBatch();
            recordsExecuted = recordsAdded;
        }
        clearParameters();
        return this;
    }


    @Override
    public void close() throws Exception {
        preparedStatement.executeBatch();
        recordsExecuted = recordsAdded;
    }

    @Override
    public void setParam(String key, Object value) {
        try {
            setParam(PARAMS.valueOf(key), value);
        }catch (IllegalArgumentException e){
            super.setParam(key, value);
        }
    }

    public long getRecordsAdded() {
        return recordsAdded;
    }

    public long getRecordsExecuted() {
        return recordsExecuted;
    }

    public void setParam(PARAMS key, Object value) {
        if (key == PARAMS.batchSize && value != null){
            if (value instanceof Number)
                batchSize = ((Number)value).longValue();
            else if (value instanceof String)
                batchSize = Long.valueOf((String)value);
        }
    }
}

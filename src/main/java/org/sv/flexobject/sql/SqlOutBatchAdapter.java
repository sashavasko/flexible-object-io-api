package org.sv.flexobject.sql;

import java.sql.PreparedStatement;
import java.util.Map;

public class SqlOutBatchAdapter extends SqlOutAdapter implements AutoCloseable {

    protected long recordsAdded = 0;
    protected long recordsExecuted = 0;
    protected long batchSize = 1000;


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
        }
        clearParameters();
        return this;
    }


    @Override
    public void close() throws Exception {
        preparedStatement.executeBatch();
        recordsExecuted = recordsAdded;
    }
}

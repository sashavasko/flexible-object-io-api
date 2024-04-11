package org.sv.flexobject.sql;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.sql.Statement.EXECUTE_FAILED;

public class SqlOutBatchAdapter extends SqlOutAdapter {

    public enum PARAMS {
        batchSize
    }

    protected long recordsAdded = 0;
    protected long recordsExecuted = 0;
    protected long recordsUpdated = 0;
    protected long commandsExecuted = 0;
    protected List<Exception> errors = new ArrayList<>();
    /*
     * Databases that will stop batch on first exception :
     *    Oracle
     * Databases that will continue batch after first exception :
     */

    protected boolean stopsOnFailure = false;
    protected long batchSize = 30; // large batch sizes use more memory without much improvement in performance

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

    public static SQLException getBatchException(BatchUpdateException batchUpdateException){
        return batchUpdateException.getNextException() == null ? batchUpdateException : batchUpdateException.getNextException();
    }

    protected void handleUpdateCounts(int[] updateCounts, BatchUpdateException batchUpdateException) throws SQLException{
        boolean continuedOnFailure = false;
        if (updateCounts != null) {
            commandsExecuted += updateCounts.length;
            for (int updateCount : updateCounts) {
                if (updateCount >= 0)
                    recordsUpdated += updateCount;
                if (updateCount == EXECUTE_FAILED && batchUpdateException != null) {
                    errors.add(getBatchException(batchUpdateException));
                    continuedOnFailure = true;
                }
            }
        }
        if (batchUpdateException != null && !continuedOnFailure){
            stopsOnFailure = true;
            throw getBatchException(batchUpdateException);
        }
    }

    protected void executeBatch() throws SQLException {
        try {
            handleUpdateCounts(preparedStatement.executeBatch(), null);
        } catch (BatchUpdateException batchUpdateException){
            handleUpdateCounts(batchUpdateException.getUpdateCounts(), batchUpdateException);
        }
        recordsExecuted = recordsAdded;
    }

    @Override
    public SqlOutAdapter save() throws Exception {
        preparedStatement.addBatch();
        ++recordsAdded;
        if (recordsAdded  >= recordsExecuted + batchSize) {
            executeBatch();
        }
        clearParameters();
        return this;
    }


    @Override
    public void close() throws Exception {
        executeBatch();
        super.close();
        if (!errors.isEmpty()){
            throw errors.get(errors.size()-1);
        }
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

    public long getRecordsUpdated() {
        return recordsUpdated;
    }

    public long getCommandsExecuted() {
        return commandsExecuted;
    }

    @Override
    public List<Exception> getErrors() {
        return errors;
    }

    @Override
    public void clearErrors(){
        errors.clear();
    }

    public boolean stopsOnFailure() {
        return stopsOnFailure;
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

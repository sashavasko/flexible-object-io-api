package org.sv.flexobject.sql;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class SqlOutBatchAdapterTest {

    @Mock
    PreparedStatement preparedStatement;

    Map<String, Integer> paramNamesXref = new HashMap();
    SqlOutBatchAdapter adapter;

    @Before
    public void setUp() throws Exception {
        paramNamesXref.put("field", 1);
        paramNamesXref.put("anotherfield", 2);
        adapter = new SqlOutBatchAdapter(preparedStatement, paramNamesXref, 2);
    }

    @Test
    public void constructorWithoutPS() {
        adapter = new SqlOutBatchAdapter(paramNamesXref, 2);
        assertEquals(2, adapter.getParamIndex("anotherfield"));
    }

    @Test
    public void save() throws Exception {
        adapter.setString("field", "blah");

        assertTrue(adapter.shouldSave());

        adapter.save();

        assertFalse(adapter.shouldSave());
        assertEquals(1, adapter.getRecordsAdded());
        assertEquals(0, adapter.getRecordsExecuted());

        adapter.setString("field", "meh");
        assertTrue(adapter.shouldSave());

        adapter.save();

        assertFalse(adapter.shouldSave());
        assertEquals(2, adapter.getRecordsAdded());
        assertEquals(2, adapter.getRecordsExecuted());
        Mockito.verify(preparedStatement).executeBatch();

    }

    @Test
    public void close() throws Exception {
        adapter.setString("field", "blah");
        adapter.save();

        assertEquals(1, adapter.getRecordsAdded());
        assertEquals(0, adapter.getRecordsExecuted());

        adapter.close();
        Mockito.verify(preparedStatement).executeBatch();
        assertEquals(1, adapter.getRecordsExecuted());


    }

    @Test
    public void setParam() throws Exception {
        adapter = new SqlOutBatchAdapter();

        adapter.setParam(SqlOutAdapter.PARAMS.preparedStatement.name(), preparedStatement);
        adapter.setParam(SqlOutAdapter.PARAMS.paramNamesXref.name(), paramNamesXref);
        adapter.setParam(SqlOutBatchAdapter.PARAMS.batchSize.name(), "1");

        adapter.setString("field", "first");
        adapter.save();

        assertEquals(1, adapter.getRecordsExecuted());

        adapter.setParam(SqlOutBatchAdapter.PARAMS.batchSize.name(), 2);

        adapter.setString("field", "second");
        adapter.save();

        assertEquals(1, adapter.getRecordsExecuted());

        adapter.setString("field", "third");
        adapter.save();

        assertEquals(3, adapter.getRecordsExecuted());
    }
}
package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class BatchInputFormatTest {

    @Mock
    BatchInputConf mockConf;

    BatchInputFormat format;
    @Before
    public void setUp() throws Exception {
        format = Mockito.mock(BatchInputFormat.class, Mockito.CALLS_REAL_METHODS);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void makeInputConf() {
        InstanceFactory.set(BatchInputConf.class, mockConf);

        assertSame(mockConf, format.makeInputConf());
    }
}
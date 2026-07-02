package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class BatchInputFormatTest {

    BatchInputConf mockConf = new BatchInputConf();

    BatchInputFormat format;
    @BeforeEach
    public void setUp() throws Exception {
        format = Mockito.mock(BatchInputFormat.class, Mockito.CALLS_REAL_METHODS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void makeInputConf() {
        InstanceFactory.set(BatchInputConf.class, mockConf);

        assertSame(mockConf, format.makeInputConf());
    }
}
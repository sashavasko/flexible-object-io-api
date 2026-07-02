package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.util.InstanceFactory;

import java.io.*;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class KeyInputSplitTest {

    @Mock
    Writable key;

    KeyInputSplit split;

    @BeforeEach
    public void setUp() throws Exception {
        split = new KeyInputSplit();
    }

    @AfterEach
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void getKey() {
        split = new KeyInputSplit(key);

        assertSame(key, split.getKey());
    }

    @Test
    public void getLength() throws IOException, InterruptedException {
        assertEquals(2l, split.getLength());
    }

    @Test
    public void getLocations() throws IOException, InterruptedException {
        assertArrayEquals(new String[0], split.getLocations());
    }

    @Test
    public void readWriteLongKeyInputSplit() throws IOException {
        split = new KeyInputSplit.LongKeySplit(new LongWritable(77777l));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        split.write(new DataOutputStream(output));

        ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());

        KeyInputSplit splitIn = new KeyInputSplit.LongKeySplit();
        splitIn.readFields(new DataInputStream(input));

        assertEquals(split, splitIn);
    }

    @Test
    public void readWriteTextKeyInputSplit() throws IOException {
        split = new KeyInputSplit.TextKeySplit(new Text("foobaduba"));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        split.write(new DataOutputStream(output));

        ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());

        KeyInputSplit splitIn = new KeyInputSplit.TextKeySplit();
        splitIn.readFields(new DataInputStream(input));

        assertEquals(split, splitIn);
    }

    @Test
    public void hash() {
        split = new KeyInputSplit(key);

        assertEquals(Objects.hash(key), split.hashCode());
    }
}
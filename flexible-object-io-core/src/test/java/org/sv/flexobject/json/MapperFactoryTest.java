package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class MapperFactoryTest {

    @Mock
    ObjectMapper objectMapper;

    @Mock
    ObjectReader objectReader;

    @Mock
    ObjectWriter objectWriter;

    @After
    public void tearDown() throws Exception {
        MapperFactory.getInstance().reset();
    }

    @Test
    public void getInstance() {
        MapperFactory instance = MapperFactory.getInstance();
        assertSame(instance, MapperFactory.getInstance());
    }

    @Test
    public void getSetObjectMapper() {
        assertNotNull(MapperFactory.getObjectMapper());

        MapperFactory.setObjectMapper(objectMapper);

        assertSame(objectMapper, MapperFactory.getObjectMapper());
    }

    @Test
    public void getSetObjectReader() {
        assertNotNull(MapperFactory.getObjectReader());

        MapperFactory.setObjectReader(objectReader);

        assertSame(objectReader, MapperFactory.getObjectReader());
    }

    @Test
    public void getSetObjectWriter() {
        assertNotNull(MapperFactory.getObjectWriter());

        MapperFactory.setObjectWriter(objectWriter);

        assertSame(objectWriter, MapperFactory.getObjectWriter());
    }

    @Test
    public void reset() {
        MapperFactory.setObjectMapper(objectMapper);
        MapperFactory.setObjectReader(objectReader);
        MapperFactory.setObjectWriter(objectWriter);

        MapperFactory.getInstance().reset();

        assertNotEquals(objectWriter, MapperFactory.getObjectWriter());
        assertNotEquals(objectReader, MapperFactory.getObjectReader());
        assertNotEquals(objectMapper, MapperFactory.getObjectMapper());
    }
}
package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.node.ValueNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.schema.DataTypes;

import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class JsonGetterTest {

    @Mock
    Function<ValueNode, Object> mockLambda;

    @Test
    public void apply() throws Exception {
        TestData test1 = new TestData("{'a':{'b':{'c':{'foo':'bar'}}}}".replace('\'', '"'));
        JsonGetter getter = new JsonGetter(TestData.class, "json", "a.b.c.foo", DataTypes.string);

        assertEquals("bar", getter.apply(test1));
    }

    @Test
    public void applyWithSpecificLambda() throws Exception {
        TestData test1 = new TestData("{'a':{'b':{'c':{'foo':'bar'}}}}".replace('\'', '"'));
        JsonGetter getter = new JsonGetter(TestData.class, "json", "a.b.c.foo", mockLambda);

        getter.apply(test1);

        verify(mockLambda).apply((ValueNode) test1.json.get("a").get("b").get("c").get("foo"));
    }


}
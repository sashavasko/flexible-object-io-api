package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.schema.DataTypes;

import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class JsonSetterTest {

    @Mock
    Function<Object, JsonNode> mockLambda;

    @Test
    public void accept() throws Exception {
        TestData testData = new TestData();
        JsonSetter setter = new JsonSetter(TestData.class, "json", "a.b.foo", DataTypes.string);

        setter.accept(testData, "bar");

        assertEquals(JsonNodeFactory.instance.textNode("bar"), testData.json.get("a").get("b").get("foo"));
    }

    @Test
    public void acceptWithSpecificLambda() throws Exception {
        TestData testData = new TestData();
        JsonSetter setter = new JsonSetter(TestData.class, "json", "a.b.foo", mockLambda);

        setter.accept(testData, "bar");

        verify(mockLambda).apply("bar");
    }

}
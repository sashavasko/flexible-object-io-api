package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.Test;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ValueType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OptionsInAdapterTest {

    public static class CliTest1 extends StreamableWithSchema {
        String key;
        String level;
    }

    @Test
    public void simpleCli() throws Exception {
        Options options = new Options();
        options.addOption("k", "key", true, "key");
        options.addOption("l", "level", true, "level");

        CommandLine cli = new DefaultParser().parse(options, new String[]{"-k", "foo", "-l", "bar"}, true);

        CliTest1 data = new CliTest1();

        new OptionsInAdapter(cli).consume(data::load);

        assertEquals("foo", data.key);
        assertEquals("bar", data.level);
    }
    public static class CliTest2 extends StreamableWithSchema {
        String key;
        JsonNode level;
    }

    @Test
    public void arrayCli() throws Exception {
        Options options = new Options();
        options.addOption("k", "key", true, "key");
        options.addOption(Option.builder().option("l").longOpt("level").valueSeparator(',').hasArg().numberOfArgs(2).build());

        CommandLine cli = new DefaultParser().parse(options, new String[]{"-k", "foo", "-l", "bar1,bar2"}, true);

        CliTest2 data = new CliTest2();

        new OptionsInAdapter(cli).consume(data::load);

        assertEquals("foo", data.key);
        assertTrue(data.level instanceof ArrayNode);
        assertEquals("bar1", data.level.get(0).asText());
        assertEquals("bar2", data.level.get(1).asText());
    }

    public static class CliTest2List extends StreamableWithSchema {
        String key;
        @ValueType(type= DataTypes.string)
        List<String> level = new ArrayList<>();
    }
    @Test
    public void arrayCliList() throws Exception {
        Options options = new Options();
        options.addOption("k", "key", true, "key");
        options.addOption(Option.builder().option("l").longOpt("level").valueSeparator(',').hasArg().numberOfArgs(2).build());

        CommandLine cli = new DefaultParser().parse(options, new String[]{"-k", "foo", "-l", "bar1,bar2"}, true);

        CliTest2List data = new CliTest2List();

        new OptionsInAdapter(cli).consume(data::load);

        assertEquals("foo", data.key);
        assertEquals("bar1", data.level.get(0));
        assertEquals("bar2", data.level.get(1));
    }

    public static class CliTestMap extends StreamableWithSchema {
        String key;
        @ValueType(type=DataTypes.string)
        Map<String, String> D = new HashMap<>();
    }

    @Test
    public void map() throws Exception {
        Options options = new Options();
        options.addOption("k", "key", true, "key");
        options.addOption(Option.builder()
                .longOpt("D")
                .argName("property=value" )
                .valueSeparator()
                .hasArg()
                .numberOfArgs(2)
                .build());

        CommandLine cli = new DefaultParser().parse(options, new String[]{"-k", "foo", "-Dparam1=val10", "-Dparam2=val20"}, true);

        CliTestMap data = new CliTestMap();

        new OptionsInAdapter(cli).consume(data::load);

        assertEquals("foo", data.key);
        assertEquals("val10", data.D.get("param1"));
        assertEquals("val20", data.D.get("param2"));
    }
}
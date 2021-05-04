package org.sv.flexobject.hadoop.streaming.parquet;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ParquetFilterParserTest {

    @Test
    public void parse() throws Exception {

        assertEquals("eq(text, Binary{\"foobar\"})", ParquetFilterParser.parse("{'eq':{'binary':'text','value':'foobar'}}".replace('\'', '"')).toString());
        assertEquals("eq(intcol, 10)", ParquetFilterParser.parse("{'eq':{'int':'intcol','value':10}}".replace('\'', '"')).toString());
        assertEquals("eq(longcol, 10000000000)", ParquetFilterParser.parse("{'eq':{'long':'longcol','value':10000000000}}".replace('\'', '"')).toString());
        assertEquals("eq(boolcol, true)", ParquetFilterParser.parse("{'eq':{'boolean':'boolcol','value':'true'}}".replace('\'', '"')).toString());
        assertEquals("eq(boolcol, false)", ParquetFilterParser.parse("{'eq':{'boolean':'boolcol','value':'false'}}".replace('\'', '"')).toString());
        assertEquals("lt(doublecol, 0.12345)", ParquetFilterParser.parse("{'lt':{'double':'doublecol','value':0.123450}}".replace('\'', '"')).toString());


        assertEquals("and(eq(intcol, 10), eq(colb, 15))", ParquetFilterParser.parse("{'and':[{'eq':{'int':'intcol','value':10}}, {'eq':{'int':'colb','value':15}}]}".replace('\'', '"')).toString());
        assertEquals("and(eq(intcol, 10), or(eq(colc, 15), eq(cold, 15)))", ParquetFilterParser.parse("{'and':[{'eq':{'int':'intcol','value':10}}, {'or':[{'eq':{'int':'colc','value':15}},{'eq':{'int':'cold','value':15}}]}]}".replace('\'', '"')).toString());
    }

    @Test
    public void parseNulls() throws IOException {
        assertEquals("noteq(col, null)", ParquetFilterParser.parse("{'notEq':{'int':'col','value':null}}".replace('\'', '"')).toString());
        assertEquals("noteq(col, null)", ParquetFilterParser.parse("{'notEq':{'string':'col','value':null}}".replace('\'', '"')).toString());
        assertEquals("noteq(col, null)", ParquetFilterParser.parse("{'notEq':{'long':'col','value':null}}".replace('\'', '"')).toString());
        assertEquals("noteq(col, null)", ParquetFilterParser.parse("{'notEq':{'boolean':'col','value':null}}".replace('\'', '"')).toString());
        assertEquals("noteq(col, null)", ParquetFilterParser.parse("{'notEq':{'double':'col','value':null}}".replace('\'', '"')).toString());
    }

}
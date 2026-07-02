package org.sv.flexobject.hadoop.mapreduce.util.cacheable;

import org.junit.jupiter.api.Test;

import java.net.URI;

public class HadoopCacheableTest {

//    @Ignore
    @Test
    public void testURIParsing() throws Exception {
        URI uri = new URI("hdfs://foo.bar.us:8020/som/path/0_0_0.parquet#something");

        System.out.println(uri);
        System.out.println(uri.getFragment());
        System.out.println(uri.toString().substring(0, uri.toString().length()-1-uri.getFragment().length()));
    }
}
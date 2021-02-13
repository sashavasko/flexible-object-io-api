package org.sv.flexobject.hadoop.mapreduce.util.cacheable;

import org.junit.Test;

import java.net.URI;

public class HadoopCacheableTest {

//    @Ignore
    @Test
    public void testURIParsing() throws Exception {
        URI uri = new URI("hdfs://enthadoopad02p.d.carfax.us:8020/vlc/cave/bui/dealers_groups/0_0_0.parquet#compcodetogroup0");

        System.out.println(uri);
        System.out.println(uri.getFragment());
        System.out.println(uri.toString().substring(0, uri.toString().length()-1-uri.getFragment().length()));
    }
}
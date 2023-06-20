package org.sv.flexobject.rabbit.streaming;


import org.sv.flexobject.Streamable;

public class RabbitSink extends RabbitGenericSink<Streamable> {

    public static Builder builder(){
        return RabbitGenericSink.builder()
                .sinkType(RabbitSink.class)
                .converter(v->((Streamable)v).toJsonBytes())
                .contentEncoding("UTF-8");
    }
}

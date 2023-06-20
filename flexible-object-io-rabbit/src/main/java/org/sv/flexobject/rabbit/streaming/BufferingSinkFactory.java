package org.sv.flexobject.rabbit.streaming;


import org.sv.flexobject.Streamable;
import org.sv.flexobject.stream.sinks.BufferingSink;

import java.util.List;
import java.util.Vector;

public class BufferingSinkFactory implements ListenerSinkFactory {

    List<Streamable> buffer = new Vector<>();
    Class<? extends Streamable> defaultType;

    public BufferingSinkFactory() {
    }

    public BufferingSinkFactory(Class<? extends Streamable> defaultType) {
        this.defaultType = defaultType;
    }

    @Override
    public MessageListenerSink get() {
        MessageListenerSink sink = new MessageListenerTransform<MessageListenerSink>(new BufferingSink<>(buffer)).setDefaultType(defaultType);
        return sink;
    }

    public List<Streamable> getBuffer() {
        return buffer;
    }
}

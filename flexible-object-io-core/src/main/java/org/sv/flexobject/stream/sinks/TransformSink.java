package org.sv.flexobject.stream.sinks;


import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;

public abstract class TransformSink<SELF,INPUT,OUTPUT> implements Sink<INPUT> {

    Sink<OUTPUT> outputSink;

    public TransformSink() {
    }

    public TransformSink(Sink<OUTPUT> outputSink) {
        this.outputSink = outputSink;
    }

    public SELF setOutputSink(Sink<OUTPUT> outputSink) {
        this.outputSink = outputSink;
        return (SELF) this;
    }

    abstract public OUTPUT transform(INPUT input);

    public void transformAll(Source<INPUT> source) throws Exception {
        while (!source.isEOF())
            put(source.get());
        setEOF();
    }

    @Override
    public boolean put(INPUT value) throws Exception {
        return outputSink.put(transform(value));
    }

    @Override
    public void setEOF() {
        outputSink.setEOF();
    }

    @Override
    public boolean hasOutput() {
        return outputSink.hasOutput();
    }
}

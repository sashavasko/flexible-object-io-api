package org.sv.flexobject.stream.sinks;


import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.FunctionWithException;

public class TransformSink<SELF,INPUT,OUTPUT> implements Sink<INPUT> {

    Sink<OUTPUT> outputSink;
    FunctionWithException<INPUT, OUTPUT, Exception> transformer = (input)->(OUTPUT)input;

    public TransformSink() {
    }

    public TransformSink(Sink<OUTPUT> outputSink) {
        this.outputSink = outputSink;
    }

    public SELF setOutputSink(Sink<OUTPUT> outputSink) {
        this.outputSink = outputSink;
        return (SELF) this;
    }
    public SELF setTransform(FunctionWithException<INPUT, OUTPUT, Exception> transformer) {
        this.transformer = transformer;
        return (SELF) this;
    }

    public OUTPUT transform(INPUT input) throws Exception{
        return transformer.apply(input);
    }

    public void transformAll(Source<INPUT> source) throws Exception {
        transformAll(source, 0, Integer.MAX_VALUE);
    }

    public void transformAll(Source<INPUT> source, int skip, int limit) throws Exception {
        INPUT data;
        while ((data = source.get()) != null && limit > 0) {
            if (skip <= 0) {
                if (put(data))
                    break;
                --limit;
            } else
                --skip;
        }
        setEOF();
    }

    @Override
    public boolean put(INPUT value) throws Exception {
        OUTPUT output = transform(value);
        if (output == null)
            return true;
        return outputSink.put(output);
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

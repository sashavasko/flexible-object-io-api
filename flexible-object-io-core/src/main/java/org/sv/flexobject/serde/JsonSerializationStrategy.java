package org.sv.flexobject.serde;

import org.sv.flexobject.Streamable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.sv.flexobject.Constants.DEFAULT_CHARSET;

public class JsonSerializationStrategy implements SerializationStrategy{

    public static final SerializationStrategy JSON = new JsonSerializationStrategy();
    public static final byte[] CONTENT_TYPE = "application/json".getBytes(StandardCharsets.UTF_8);

    private JsonSerializationStrategy(){}

    @Override
    public byte[] serialize(Streamable datum) throws IOException {
        try {
            return datum.toJsonBytes(DEFAULT_CHARSET);
        }catch (IOException ioe){
            throw ioe;
        }catch (Exception e){
            throw new IOException("Failed to serialize object of class " + datum.getClass().getName(), e);
        }
    }

    @Override
    public void deserialize(Streamable datum, byte[] bytes, int offset, int length) throws IOException {
        try {
            datum.fromJsonBytes(bytes, offset, length, DEFAULT_CHARSET);
        } catch (IOException ioe){
            throw ioe;
        } catch (Exception e){
            throw new IOException("Failed to serialize object of class " + datum.getClass().getName(), e);
        }
    }

    @Override
    public byte[] getContentType() {
        return CONTENT_TYPE;
    }
}

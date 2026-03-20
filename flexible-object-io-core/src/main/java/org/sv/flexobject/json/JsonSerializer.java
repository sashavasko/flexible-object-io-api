package org.sv.flexobject.json;


import org.sv.flexobject.Streamable;
import org.sv.flexobject.io.Serializer;

import java.io.IOException;

public class JsonSerializer implements Serializer {

    public static final JsonSerializer instance = new JsonSerializer();

    @Override
    public byte[] ser(Streamable datum) throws IOException {
        try {
            return datum.toJsonBytes();
        }catch (IOException ioe){
            throw ioe;
        }catch (Exception e){
            throw new IOException("Failed to serialize object of class " + datum.getClass().getName(), e);
        }
    }

    @Override
    public void deser(Streamable datum, byte[] bytes) throws IOException {
        try {
            datum.fromJsonBytes(bytes);
        } catch (IOException ioe){
            throw ioe;
        } catch (Exception e){
            throw new IOException("Failed to serialize object of class " + datum.getClass().getName(), e);
        }
    }
}

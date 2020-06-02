package org.sv.flexobject.stream.sources;

import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.ByteRepresentable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Consumer;

public class ByteStreamSource<T extends ByteRepresentable>  implements Iterator<T>, Iterable<T>, Source<T>, AutoCloseable {
    InputStream inputStream;
    byte[] readBuffer = new byte[1024 * 10];
    Class<? extends ByteRepresentable> clazz;
    boolean closeStream = false;

    public ByteStreamSource(InputStream is, Class<? extends ByteRepresentable> clazz, boolean closeStream) {
        this.inputStream = is;
        this.clazz = clazz;
        this.closeStream = closeStream;
    }

    public ByteStreamSource(InputStream is, Class<? extends ByteRepresentable> clazz) {
        this(is, clazz, false);
    }

    @Override
    public boolean hasNext() {
        try {
            return (inputStream.available() > 0);
        } catch (IOException e) {
            return false;
        }
    }

//    public T next() {
//        try {
//            int pos = 0;
//            int b;
//            while ((b = inputStream.read()) >= 0) {
////            System.out.println(b);
//                if (b == 0x000d) {
//                    inputStream.read();
//                    break;
//                }
//                if (b == 0x000a) {
//                    break;
//                }
//                if (pos >= readBuffer.length){
//                    byte[] newBuffer = new byte[readBuffer.length*2];
//                    System.arraycopy(readBuffer, 0, newBuffer, 0, pos);
//                    readBuffer = newBuffer;
//                }
//                readBuffer[pos++] = (byte) b;
//            }
//            if (pos == 0)
//                return null;
//
//            IByteRepresentable datum = clazz.newInstance();
//            datum.fromBytes(readBuffer, pos);
//            return (T) datum;
//        } catch (IOException | InstantiationException | IllegalAccessException e) {
//            return null;
//        }
//    }


    public T next() {
        try {
            int pos = 0;
            int b;
            while ((b = inputStream.read()) >= 0) {
//            System.out.println(b);
                if (b == 0x000d) {
                    int bb = inputStream.read();
                    if (bb >= 0) {
                        if (bb != 0x000a)
                            pos = consumeByte(pos, (byte) b);
                        b = bb;
                    }else
                        break;
                }
                if (b == 0x000a) {
                    break;
                }
                pos = consumeByte(pos, (byte) b);
            }
            if (pos == 0)
                return null;

            ByteRepresentable datum = clazz.newInstance();
            datum.fromBytes(readBuffer, pos);
            return (T) datum;
        } catch (IOException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    private int consumeByte(int pos, byte b) {
        if (pos >= readBuffer.length){
            byte[] newBuffer = new byte[readBuffer.length*2];
            System.arraycopy(readBuffer, 0, newBuffer, 0, pos);
            readBuffer = newBuffer;
        }
        readBuffer[pos++] = b;
        return pos;
    }

    @Override
    public void remove() {

    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
        T split;
        while ((split = next()) != null)
            action.accept(split);
    }


    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    synchronized public T get() throws Exception {
        return hasNext() ? next() : null;
    }

    @Override
    public boolean isEOF() {
        return !hasNext();
    }

    @Override
    public void close() throws Exception {
        if (closeStream)
            inputStream.close();
    }
}

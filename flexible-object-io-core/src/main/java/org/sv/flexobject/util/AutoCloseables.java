package org.sv.flexobject.util;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class AutoCloseables {

    public static int closeQuietly(Object ... closeables) {
        int countClosed = 0;
        for (Object closeable : closeables) {
            if (closeQuietly(closeable))
                countClosed++;
        }
        return countClosed;
    }

    public static boolean closeQuietly(Object closeable) {
        if (closeable != null && closeable instanceof AutoCloseable) {
            try {
                ((AutoCloseable)closeable).close();
                return true;
            } catch (Exception ignore) {
            }
        }
        return false;
    }

    public static int close(Object ... closeables) throws Exception {
        int countClosed = 0;
        for (Object closeable : closeables) {
            if (close(closeable))
                countClosed++;
        }
        return countClosed;
    }

    public static boolean close(Object closeable) throws Exception {
        if (closeable != null && closeable instanceof AutoCloseable) {
                ((AutoCloseable)closeable).close();
                return true;
        }
        return false;
    }

    public static void closeOr(Object closeable, Procedure or) throws Exception {
        if (closeable != null){
            if( closeable instanceof AutoCloseable)
                ((AutoCloseable)closeable).close();
            else
                or.invoke();
        }
    }

    public static int close(Consumer<Exception> exceptionHandler, Object ... closeables) {
        int countClosed = 0;
        for (Object closeable : closeables) {
            if (close(exceptionHandler, closeable))
                countClosed++;
        }
        return countClosed;
    }

    public static boolean close(Consumer<Exception> exceptionHandler, Object closeable) {
        if (closeable != null && closeable instanceof AutoCloseable) {
            try {
                ((AutoCloseable)closeable).close();
            } catch (Exception e) {
                exceptionHandler.accept(e);
            }
            return true;
        }
        return false;
    }

    public static int closeIO(Object ... closeables) throws IOException {
        int countClosed = 0;
        for (Object closeable : closeables) {
            if (closeIO(closeable))
                countClosed++;
        }
        return countClosed;
    }

    public static boolean closeIO(Object closeable) throws IOException {
        if (closeable != null && closeable instanceof AutoCloseable) {
            try {
                ((AutoCloseable)closeable).close();
            } catch (Exception e) {
                if (e instanceof IOException)
                    throw (IOException)e;
                throw new IOException(e);
            }
            return true;
        }
        return false;
    }

}

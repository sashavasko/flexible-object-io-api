package org.sv.flexobject.translate;

import java.util.Objects;
import java.util.function.Function;

public interface Translator extends Function<String, String> {

    default Translator compose(Translator before) {
        Objects.requireNonNull(before);
        return (v) -> this.apply(before.apply(v));
    }

    default Translator andThen(Translator after) {
        Objects.requireNonNull(after);
        return (t) -> after.apply(this.apply(t));
    }

    class Identity implements Translator{
        @Override
        public String apply(String s) {
            return s;
        }
    }

    Identity identity = new Identity();
    static Translator identity(){
        return identity;
    }
}

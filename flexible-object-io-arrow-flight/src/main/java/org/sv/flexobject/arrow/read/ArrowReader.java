package org.sv.flexobject.arrow.read;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;

import java.util.Map;

public abstract class ArrowReader {
    Map<Long, Dictionary> dictionaryMap;

    public ArrowReader() {
    }

    public void setDictionaryMap(Map<Long, Dictionary> dictionaryMap) {
        this.dictionaryMap = dictionaryMap;
    }

    public ValueVector decodeVector(ValueVector vector) {
        if (dictionaryMap == null)
            return vector;
        long dictionaryId = vector.getField().getDictionary().getId();
        return DictionaryEncoder.decode(vector, dictionaryMap.get(dictionaryId));
    }

    public abstract boolean isNull(int rowIndex);
    public abstract Object read(int rowIndex);

    public Map<Long, Dictionary> getDictionaryMap() {
        return dictionaryMap;
    }
}

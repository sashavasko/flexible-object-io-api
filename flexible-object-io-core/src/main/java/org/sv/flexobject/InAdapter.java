package org.sv.flexobject;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.util.ConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Set;

/**
 * Implementations provide uniform interface to reading various input formats and or medias.
 * This allows for a standard interface to inputs of dramatically different architecture, such as files, Kafka topics, Databases.
 */
public interface InAdapter extends AutoCloseable, Parameterized{

    String getString(String fieldName) throws Exception;

    JsonNode getJson(String fieldName) throws Exception;

    Integer getInt(String fieldName) throws Exception;

    /**
     * Retrieves the value of an Integer field specified by fieldName.
     * @param fieldName - high level name of the field known to the consumer
     * @param defaultValue - default value
     * @return value of the field or defaultValue if the field is unset/null
     * @throws Exception - actual exception thrown is implementation specific
     */
    default int getOptionalInt(String fieldName, int defaultValue) throws Exception{
        Integer value = getInt(fieldName);
        return value == null ? defaultValue : value;
    }

    Boolean getBoolean(String fieldName) throws Exception;

    /**
     * Retrieves the value of a Boolean field specified by fieldName.
     * @param fieldName - high level name of the field known to the consumer
     * @return value of the field or false if the field is unset/null
     * @throws Exception - actual exception thrown is implementation specific
     */
    default boolean getOptionalBoolean(String fieldName) throws Exception{
        Boolean value = getBoolean(fieldName);
        return value != null && value;
    }

    Long getLong(String fieldName) throws Exception;

    /**
     * Retrieves the value of a Long field specified by fieldName.
     * @param fieldName - high level name of the field known to the consumer
     * @param defaultValue - default value
     * @return value of the field or defaultValue if the field is unset/null
     * @throws Exception - actual exception thrown is implementation specific
     */
    default long getOptionalLong(String fieldName, long defaultValue) throws Exception{
        Long value = getLong(fieldName);
        return value == null ? defaultValue : value;
    }

    Double getDouble(String fieldName) throws Exception;

    /**
     * Retrieves the value of a Long field specified by fieldName.
     * @param fieldName - high level name of the field known to the consumer
     * @param defaultValue - default value
     * @return value of the field or defaultValue if the field is unset/null
     * @throws Exception - actual exception thrown is implementation specific
     */
    default double getOptionalDouble(String fieldName, double defaultValue) throws Exception{
        Double value = getDouble(fieldName);
        return value == null ? defaultValue : value;
    }

    Date getDate(String fieldName) throws Exception;
    Timestamp getTimestamp(String fieldName) throws Exception;

    /**
     * Retrieves the value of a Date field specified by fieldName converted to LocalDate
     * @param fieldName - high level name of the field known to the consumer
     * @return value of the field
     * @throws Exception - actual exception thrown is implementation specific
     */
    default LocalDate getLocalDate(String fieldName) throws Exception {
        Date date = getDate(fieldName);
        return date != null ? date.toLocalDate() : null;
    }

    default Class<?> getClass(String fieldName) throws Exception {
        String className = getString(fieldName);
        return DataTypes.classConverter(className);
    }

    default Class<?> getOptionalClass(String fieldName, Class<?> defaultValue) throws Exception {
        Class<?> clazz = getClass(fieldName);
        return clazz == null ? defaultValue : clazz;
    }

    default <T extends Enum<T>> T getEnum(String fieldName, T defaultValue) throws Exception {
        String valueName = getString(fieldName);
        return DataTypes.enumConverter(valueName, defaultValue);
    }

    default Enum getEnum(String fieldName, Class<? extends Enum> enumClass) throws Exception {
        String valueName = getString(fieldName);
        return DataTypes.enumConverter(valueName, enumClass);
    }

    default Set<Enum> getEnumSet(String fieldName, Class<? extends Enum> enumClass, String emptyValue) throws Exception {
        String valueName = getString(fieldName);
        return DataTypes.enumSetConverter(valueName, enumClass, emptyValue);
    }

    /**
     * Optionally implemented to cross-reference field name to media specific field name.
     * @param fieldName - high level name of the field known to the consumer
     * @return media specific field name
     */
    default String translateInputFieldName(String fieldName) {
        return fieldName;
    }

    /**
     * Advances to the next record to input. This must be called before any get methods.
     */
    boolean next() throws Exception;

    default boolean consume(StreamableWithSchema target) throws Exception {
        return consume(target::load);
    }

    default boolean consume(ConsumerWithException<InAdapter, Exception> targetConsumer) throws Exception {
        if (!next())
            return false;
        targetConsumer.accept(this);
        return true;
    }

    /**
     * Optionally implemented to inform the source of a successful processing of the element from the input.
     */
    default void ack(){}

    /**
     * Optionally implemented to mark the underlying input as complete.
     */
    default void setEOF() {}
}

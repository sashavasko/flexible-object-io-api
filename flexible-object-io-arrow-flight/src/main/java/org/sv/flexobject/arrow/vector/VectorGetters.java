/*
 * This is derived from org.apache.arrow.vector.GenerateSampleData
 * from Apache Arrow project licensed under the following license :
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.sv.flexobject.arrow.vector;

import org.sv.flexobject.schema.FieldDescriptor;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

/**
 * Helper class to generate test data for Nullable fixed and variable width scalar vectors. Previous
 * implementations of java vector classes provided generateTestData(now deprecated) API to populate
 * the vector with sample data. This class should be used for that purpose.
 */
public class VectorGetters {

  public static Getter getGetter(Field field, FieldDescriptor descriptor) {
        switch(field.getFieldType().getType().getTypeID()){
            case Null:
                return null;

            case Binary:
              return VectorGetters::readVarBinaryData;
            case Int:
              return VectorGetters::readIntData;
            case Bool:
              return VectorGetters::readBooleanData;
            case Utf8:
              return VectorGetters::readVarCharData;
            case Date:
              return VectorGetters::readDateDayData;
            case Time:
              return VectorGetters::readTimeMilliData;
            case Decimal:
              return VectorGetters::readDecimalData;
            case Timestamp:
              return VectorGetters::readTimeStampData;
            case FloatingPoint:
              return VectorGetters::readFloatData;
        }
    return null;
  }

  public interface Getter {
    Object get(ValueVector vector, int index) throws Exception;
  }


  private VectorGetters() {}

  /** Populates <code>vector</code> with <code>valueCount</code> random values. */
  public static Object get(final ValueVector vector, int index, final Object data) throws Exception {
    return get(vector, index,  null);
  }

  public static Object get(final ValueVector vector, int index, FieldDescriptor sourceDescriptor) throws Exception {
    Getter getter = getGetter(vector, sourceDescriptor);
    return getter == null ? null : getter.get(vector, index);
  }

  public static Getter getGetter(final ValueVector vector, FieldDescriptor sourceDescriptor) {
    if (vector instanceof IntVector) {
      return VectorGetters::readIntData;
    } else if (vector instanceof DecimalVector) {
      return VectorGetters::readDecimalData;
    } else if (vector instanceof BitVector) {
      return VectorGetters::readBooleanData;
    } else if (vector instanceof VarCharVector) {
      return VectorGetters::readVarCharData;
    } else if (vector instanceof VarBinaryVector) {
      return VectorGetters::readVarBinaryData;
    } else if (vector instanceof BigIntVector) {
      return VectorGetters::readBigIntData;
    } else if (vector instanceof Float4Vector) {
      return VectorGetters::readFloatData;
    } else if (vector instanceof Float8Vector) {
      return VectorGetters::readDoubleData;
    } else if (vector instanceof DateDayVector) {
      return VectorGetters::readDateDayData;
    } else if (vector instanceof DateMilliVector) {
      return VectorGetters::readDateMilliData;
    } else if (vector instanceof IntervalDayVector) {
      return VectorGetters::readIntervalDayData;
    } else if (vector instanceof IntervalYearVector) {
      return VectorGetters::readIntervalYearData;
    } else if (vector instanceof SmallIntVector) {
      return VectorGetters::readSmallIntData;
    } else if (vector instanceof TinyIntVector) {
      return VectorGetters::readTinyIntData;
    } else if (vector instanceof TimeMicroVector) {
      return VectorGetters::readTimeMicroData;
    } else if (vector instanceof TimeMilliVector) {
      return VectorGetters::readTimeMilliData;
    } else if (vector instanceof TimeNanoVector) {
      return VectorGetters::readTimeNanoData;
    } else if (vector instanceof TimeSecVector) {
      return VectorGetters::readTimeSecData;
    } else if (vector instanceof TimeStampSecVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampMicroVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampMilliVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampNanoVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampSecTZVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampMicroTZVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampMilliTZVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof TimeStampNanoTZVector) {
      return VectorGetters::readTimeStampData;
    } else if (vector instanceof UInt1Vector) {
      return VectorGetters::readUInt1Data;
    } else if (vector instanceof UInt2Vector) {
      return VectorGetters::readUInt2Data;
    } else if (vector instanceof UInt4Vector) {
      return VectorGetters::readUInt4Data;
    } else if (vector instanceof UInt8Vector) {
      return VectorGetters::readUInt8Data;
    }
    return null;
  }

  private static Object readTimeStampData(ValueVector valueVector, int index) throws Exception {
    TimeStampVector vector = (TimeStampVector) valueVector;
    long value = vector.get(index);
    return value == 0 ? null : new Timestamp(value);
  }

  private static Object readDecimalData(ValueVector valueVector, int index) throws Exception {
    DecimalVector vector = (DecimalVector) valueVector;
    BigDecimal value = vector.getObject(index);
    return value == null ? null : value.doubleValue();
  }

  private static Object readIntData(ValueVector valueVector, int index) throws Exception {
    IntVector vector = (IntVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readBooleanData(ValueVector valueVector, int index) throws Exception {
    BitVector vector = (BitVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readIntervalYearData(ValueVector valueVector, int index) throws Exception {
    IntervalYearVector vector = (IntervalYearVector) valueVector;
    Period value = vector.getObject(index);
    return value == null ? null : value.get(ChronoUnit.YEARS);
  }

  private static Object readIntervalDayData(ValueVector valueVector, int index) throws Exception {
    IntervalDayVector vector = (IntervalDayVector) valueVector;
    Duration value = vector.getObject(index);
    return value == null ? null : value.toDays();
//    long timeMillis = DataTypes.timestampConverter(data).getTime();
//    int days = (int) TimeUnit.MILLISECONDS.toDays(timeMillis);
//    long daysInMillis = TimeUnit.DAYS.toMillis(days);
//    vector.setSafe(index, days, (int) (timeMillis - daysInMillis));
  }

  private static Object readTimeSecData(ValueVector valueVector, int index) throws Exception {
    TimeSecVector vector = (TimeSecVector) valueVector;
    Integer value = vector.getObject(index);
    return value == null ? null : new Timestamp(TimeUnit.SECONDS.toMillis(value));
//    long timeMillis = DataTypes.timestampConverter(data).getTime();
//    int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(timeMillis);
//    vector.setSafe(index, seconds);
  }

  private static Object readTimeMilliData(ValueVector valueVector, int index) throws Exception {
    TimeMilliVector vector = (TimeMilliVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readTimeMicroData(ValueVector valueVector, int index) throws Exception {
    TimeMicroVector vector = (TimeMicroVector) valueVector;
    Long value = vector.getObject(index);
    return value == null ? null : new Timestamp(TimeUnit.MICROSECONDS.toMillis(value));
  }

  private static Object readTimeNanoData(ValueVector valueVector, int index) throws Exception {
    TimeNanoVector vector = (TimeNanoVector) valueVector;
    Long value = vector.getObject(index);
    return value == null ? null : new Timestamp(TimeUnit.NANOSECONDS.toMillis(value));
//    vector.setSafe(index, DataTypes.int64Converter(data));
  }

  private static Object readDateDayData(ValueVector valueVector, int index) throws Exception {
    DateDayVector vector = (DateDayVector) valueVector;
    Integer value = vector.getObject(index);
    return value == null ? null : LocalDate.ofEpochDay(value);
//    LocalDate date = DataTypes.localDateConverter(data);
//    vector.setSafe(index, (int) date.toEpochDay());
  }

  private static Object readDateMilliData(ValueVector valueVector, int index) throws Exception {
    DateMilliVector vector = (DateMilliVector) valueVector;
    return vector.getObject(index);
//    LocalDate date = DataTypes.localDateConverter(data);
//    long milli = (int) TimeUnit.DAYS.toMillis(date.toEpochDay());
//    vector.setSafe(index, milli);
  }

  private static Object readSmallIntData(ValueVector valueVector, int index) throws Exception {
    SmallIntVector vector = (SmallIntVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readTinyIntData(ValueVector valueVector, int index) throws Exception {
    TinyIntVector vector = (TinyIntVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readUInt1Data(ValueVector valueVector, int index) throws Exception {
    UInt1Vector vector = (UInt1Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readUInt2Data(ValueVector valueVector, int index) throws Exception {
    UInt2Vector vector = (UInt2Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readUInt4Data(ValueVector valueVector, int index) throws Exception {
    UInt4Vector vector = (UInt4Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readUInt8Data(ValueVector valueVector, int index) throws Exception {
    UInt8Vector vector = (UInt8Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readBigIntData(ValueVector valueVector, int index) throws Exception {
    BigIntVector vector = (BigIntVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readFloatData(ValueVector valueVector, int index) throws Exception {
    Float4Vector vector = (Float4Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readDoubleData(ValueVector valueVector, int index) throws Exception {
    Float8Vector vector = (Float8Vector) valueVector;
    return vector.getObject(index);
  }

  private static Object readVarBinaryData(ValueVector valueVector, int index) throws Exception {
    VarBinaryVector vector = (VarBinaryVector) valueVector;
    return vector.getObject(index);
  }

  private static Object readVarCharData(ValueVector valueVector, int index) throws Exception {
    VarCharVector vector = (VarCharVector) valueVector;
    Text value = vector.getObject(index);
    return value == null ? null : value.toString();
  }
}

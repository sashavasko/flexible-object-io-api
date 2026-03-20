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

import com.carfax.dt.streaming.schema.DataTypes;
import com.carfax.dt.streaming.schema.FieldDescriptor;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.concurrent.TimeUnit;

/**
 * Helper class to generate test data for Nullable fixed and variable width scalar vectors. Previous
 * implementations of java vector classes provided generateTestData(now deprecated) API to populate
 * the vector with sample data. This class should be used for that purpose.
 */
public class VectorSetters {

  public static Setter getSetter(Field field, FieldDescriptor descriptor) {
        switch(field.getFieldType().getType().getTypeID()){
            case Null:
                return null;

            case Binary:
              return VectorSetters::writeVarBinaryData;
            case Int:
              return VectorSetters::writeIntData;
            case Bool:
              return VectorSetters::writeBooleanData;
            case Utf8:
              return VectorSetters::writeVarCharData;
            case Date:
              return VectorSetters::writeDateDayData;
            case Time:
              return VectorSetters::writeTimeMilliData;
            case Decimal:
              return VectorSetters::writeDecimalData;
            case Timestamp:
              return VectorSetters::writeTimeStampData;
            case FloatingPoint:
              return VectorSetters::writeFloatData;
        }
    return null;
  }

  public interface Setter {
    void accept(ValueVector vector, int index, Object data) throws Exception;
  }


  private VectorSetters() {}

  /** Populates <code>vector</code> with <code>valueCount</code> random values. */
  public static void set(final ValueVector vector, int index, final Object data) throws Exception {
    set(vector, index, data, null);
  }

  public static void set(final ValueVector vector, int index, final Object data, FieldDescriptor sourceDescriptor) throws Exception {
    Setter setter = getSetter(vector, sourceDescriptor);
    if (setter != null) {
      setter.accept(vector, index, data);
    }
  }

  public static Setter getSetter(final ValueVector vector, FieldDescriptor sourceDescriptor) {
    if (vector instanceof IntVector) {
      return VectorSetters::writeIntData;
    } else if (vector instanceof DecimalVector) {
      return VectorSetters::writeDecimalData;
    } else if (vector instanceof BitVector) {
      return VectorSetters::writeBooleanData;
    } else if (vector instanceof VarCharVector) {
      return VectorSetters::writeVarCharData;
    } else if (vector instanceof VarBinaryVector) {
      return VectorSetters::writeVarBinaryData;
    } else if (vector instanceof BigIntVector) {
      return VectorSetters::writeBigIntData;
    } else if (vector instanceof Float4Vector) {
      return VectorSetters::writeFloatData;
    } else if (vector instanceof Float8Vector) {
      return VectorSetters::writeDoubleData;
    } else if (vector instanceof DateDayVector) {
      return VectorSetters::writeDateDayData;
    } else if (vector instanceof DateMilliVector) {
      return VectorSetters::writeDateMilliData;
    } else if (vector instanceof IntervalDayVector) {
      return VectorSetters::writeIntervalDayData;
    } else if (vector instanceof IntervalYearVector) {
      return VectorSetters::writeIntervalYearData;
    } else if (vector instanceof SmallIntVector) {
      return VectorSetters::writeSmallIntData;
    } else if (vector instanceof TinyIntVector) {
      return VectorSetters::writeTinyIntData;
    } else if (vector instanceof TimeMicroVector) {
      return VectorSetters::writeTimeMicroData;
    } else if (vector instanceof TimeMilliVector) {
      return VectorSetters::writeTimeMilliData;
    } else if (vector instanceof TimeNanoVector) {
      return VectorSetters::writeTimeNanoData;
    } else if (vector instanceof TimeSecVector) {
      return VectorSetters::writeTimeSecData;
    } else if (vector instanceof TimeStampSecVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampMicroVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampMilliVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampNanoVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampSecTZVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampMicroTZVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampMilliTZVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof TimeStampNanoTZVector) {
      return VectorSetters::writeTimeStampData;
    } else if (vector instanceof UInt1Vector) {
      return VectorSetters::writeUInt1Data;
    } else if (vector instanceof UInt2Vector) {
      return VectorSetters::writeUInt2Data;
    } else if (vector instanceof UInt4Vector) {
      return VectorSetters::writeUInt4Data;
    } else if (vector instanceof UInt8Vector) {
      return VectorSetters::writeUInt8Data;
    }
    return null;
  }

  private static void writeTimeStampData(ValueVector valueVector, int index, Object data) throws Exception {
    TimeStampVector vector = (TimeStampVector) valueVector;
    vector.setSafe(index, DataTypes.timestampConverter(data).getTime());
  }

  private static void writeDecimalData(ValueVector valueVector, int index, Object data) throws Exception {
    DecimalVector vector = (DecimalVector) valueVector;
    vector.setSafe(index, new BigDecimal(DataTypes.float64Converter(data)));
  }

  private static void writeIntData(ValueVector valueVector, int index, Object data) throws Exception {
    IntVector vector = (IntVector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeBooleanData(ValueVector valueVector, int index, Object data) throws Exception {
    BitVector vector = (BitVector) valueVector;
    vector.setSafe(index, DataTypes.boolConverter(data) ? 1 : 0);
  }

  private static void writeIntervalYearData(ValueVector valueVector, int index, Object data) throws Exception {
    IntervalYearVector vector = (IntervalYearVector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeIntervalDayData(ValueVector valueVector, int index, Object data) throws Exception {
    IntervalDayVector vector = (IntervalDayVector) valueVector;
    long timeMillis = DataTypes.timestampConverter(data).getTime();
    int days = (int) TimeUnit.MILLISECONDS.toDays(timeMillis);
    long daysInMillis = TimeUnit.DAYS.toMillis(days);
    vector.setSafe(index, days, (int) (timeMillis - daysInMillis));
  }

  private static void writeTimeSecData(ValueVector valueVector, int index, Object data) throws Exception {
    TimeSecVector vector = (TimeSecVector) valueVector;
    long timeMillis = DataTypes.timestampConverter(data).getTime();
    int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(timeMillis);
    vector.setSafe(index, seconds);
  }

  private static void writeTimeMilliData(ValueVector valueVector, int index, Object data) throws Exception {
    TimeMilliVector vector = (TimeMilliVector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeTimeMicroData(ValueVector valueVector, int index, Object data) throws Exception {
    TimeMicroVector vector = (TimeMicroVector) valueVector;
    vector.setSafe(index, DataTypes.int64Converter(data));
  }

  private static void writeTimeNanoData(ValueVector valueVector, int index, Object data) throws Exception {
    TimeNanoVector vector = (TimeNanoVector) valueVector;
    vector.setSafe(index, DataTypes.int64Converter(data));
  }

  private static void writeDateDayData(ValueVector valueVector, int index, Object data) throws Exception {
    DateDayVector vector = (DateDayVector) valueVector;
    LocalDate date = DataTypes.localDateConverter(data);
    vector.setSafe(index, (int) date.toEpochDay());
  }

  private static void writeDateMilliData(ValueVector valueVector, int index, Object data) throws Exception {
    DateMilliVector vector = (DateMilliVector) valueVector;
    LocalDate date = DataTypes.localDateConverter(data);
    long milli = (int) TimeUnit.DAYS.toMillis(date.toEpochDay());
    vector.setSafe(index, milli);
  }

  private static void writeSmallIntData(ValueVector valueVector, int index, Object data) throws Exception {
    SmallIntVector vector = (SmallIntVector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeTinyIntData(ValueVector valueVector, int index, Object data) throws Exception {
    TinyIntVector vector = (TinyIntVector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeUInt1Data(ValueVector valueVector, int index, Object data) throws Exception {
    UInt1Vector vector = (UInt1Vector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeUInt2Data(ValueVector valueVector, int index, Object data) throws Exception {
    UInt2Vector vector = (UInt2Vector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeUInt4Data(ValueVector valueVector, int index, Object data) throws Exception {
    UInt4Vector vector = (UInt4Vector) valueVector;
    vector.setSafe(index, DataTypes.int32Converter(data));
  }

  private static void writeUInt8Data(ValueVector valueVector, int index, Object data) throws Exception {
    UInt8Vector vector = (UInt8Vector) valueVector;
    vector.setSafe(index, DataTypes.int64Converter(data));
  }

  private static void writeBigIntData(ValueVector valueVector, int index, Object data) throws Exception {
    BigIntVector vector = (BigIntVector) valueVector;
    vector.setSafe(index, DataTypes.int64Converter(data));
  }

  private static void writeFloatData(ValueVector valueVector, int index, Object data) throws Exception {
    Float4Vector vector = (Float4Vector) valueVector;
    vector.setSafe(index, DataTypes.float64Converter(data).floatValue());
  }

  private static void writeDoubleData(ValueVector valueVector, int index, Object data) throws Exception {
    Float8Vector vector = (Float8Vector) valueVector;
    vector.setSafe(index, DataTypes.float64Converter(data));
  }

  private static void writeVarBinaryData(ValueVector valueVector, int index, Object data) throws Exception {
    VarBinaryVector vector = (VarBinaryVector) valueVector;
    vector.setSafe(index, DataTypes.binaryConverter(data));
  }

  private static void writeVarCharData(ValueVector valueVector, int index, Object data) throws Exception {
    VarCharVector vector = (VarCharVector) valueVector;
    vector.setSafe(index, new Text(DataTypes.stringConverter(data)));
  }
}

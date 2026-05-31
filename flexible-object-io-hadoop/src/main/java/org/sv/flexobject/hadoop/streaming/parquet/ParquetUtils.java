package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.ParquetRuntimeException;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ParquetUtils {
    public static final int JULIAN_EPOCH_DAY = 4881176;
    public static final int PARQUET_EPOCH_DAY = 0; //Apache Drill prior to v 1.9 used Julian Date instead of proper UNix epoch 4881176;

    public static Comparable calculateLongBinaryMax(Comparable x1, Comparable x2){
        if (x1 == null)
            return x2;

        Binary b1 = (Binary) x1;
        Binary b2 = (Binary) x2;

        return (Long.valueOf(b2.toStringUsingUTF8()).compareTo(Long.valueOf(b1.toStringUsingUTF8())) > 0) ? x2 : x1;
    }

    public static Comparable calculateLongBinaryMin(Comparable x1, Comparable x2){
        if (x1 == null)
            return x2;

        Binary b1 = (Binary) x1;
        Binary b2 = (Binary) x2;

        return (Long.valueOf(b2.toStringUsingUTF8()).compareTo(Long.valueOf(b1.toStringUsingUTF8())) < 0) ? x2 : x1;
    }
    public static final BiFunction<Comparable, Comparable, Comparable> calculateMax = (x1, x2) -> x1 == null || x2.compareTo(x1) > 0 ? x2 : x1;
    public static final BiFunction<Comparable, Comparable, Comparable> calculateMin = (x1, x2) -> x1 == null || x2.compareTo(x1) < 0 ? x2 : x1;
    public static final Function<Statistics, Comparable> getMax = (stats)->stats.genericGetMax();
    public static final Function<Statistics, Comparable> getMin = (stats)->stats.genericGetMin();
    public static final Function<Statistics, Comparable> getNumNulls = (stats)->stats.getNumNulls();

    public static Comparable getMaxValueInFile(Configuration conf, Path path, String column) throws IOException {
        return getValueInFile(conf, path, column, getMax, calculateMax);
    }

    public static Comparable getMinValueInFile(Configuration conf, Path path, String column) throws IOException {
        return getValueInFile(conf, path, column, getMin, calculateMin);
    }

    public static Comparable getValueInFile(Configuration conf, Path path, String column, Function<Statistics, Comparable> getter, BiFunction<Comparable, Comparable, Comparable> calculator) throws IOException {
        Comparable result = null;
        ParquetMetadata footer = ParquetFileReader.readFooter(
                conf, path, ParquetMetadataConverter.NO_FILTER);
        MessageType schema = footer.getFileMetaData().getSchema();

//        int columnIdx = schema.getFieldIndex(column);
        ColumnPath columnPath = ColumnPath.fromDotString(column);

        List<BlockMetaData> rowGroups = footer.getBlocks();
        for (int index = 0; index < rowGroups.size(); index += 1) {
            BlockMetaData blockMetaData = rowGroups.get(index);
            List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
            Optional<ColumnChunkMetaData> columnChunkMetaData = findColumnWithPath(columns, columnPath);
            if (columnChunkMetaData.isPresent()) {
                Statistics statistics = columnChunkMetaData.get().getStatistics();

                Comparable blockMaxValue = getter.apply(statistics);
                result = calculator.apply(result, blockMaxValue);
            } else {
                throw new ParquetRuntimeException("Failed to find field " + column + " in schema: " + schema){
                };
            }
        }
        return result;
    }

    private static Optional<ColumnChunkMetaData> findColumnWithPath(List<ColumnChunkMetaData> columns, ColumnPath columnPath) {
        for (ColumnChunkMetaData columnChunkMetaData : columns){
            ColumnPath path = columnChunkMetaData.getPath();
            if (columnPath.equals(path))
                return Optional.of(columnChunkMetaData);
        }
        return Optional.empty();
    }

    public static Comparable getMaxValueInFiles(Configuration conf, Path path, boolean recursive, String column) throws IOException {
        return getValueInFiles(conf, path, recursive, column, getMax, calculateMax);
    }

    public static Comparable getMinValueInFiles(Configuration conf, Path path, boolean recursive, String column) throws IOException {
        return getValueInFiles(conf, path, recursive, column, getMin, calculateMin);
    }

    public static Comparable getValueInFiles(Configuration conf, Path path, boolean recursive, String column, Function<Statistics, Comparable> getter, BiFunction<Comparable, Comparable, Comparable> calculator) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        RemoteIterator<LocatedFileStatus> iter = fs.listFiles(path, recursive);
        Comparable result = null;
        while (iter.hasNext()){
            LocatedFileStatus lfs  = iter.next();
            if (lfs.isFile() && lfs.getPath().getName().endsWith(".parquet")){
//                System.out.println("Checking file " + lfs.getPath());
                Comparable fileValue = getValueInFile(conf, lfs.getPath(), column, getter, calculator);
                result = calculator.apply(result, fileValue);
//                System.out.println("New result is " + result);
            }
        }
        return result;
    }

    public static long parquetDateToMillis(int parquetDate){
        if (parquetDate > JULIAN_EPOCH_DAY)
            return (parquetDate - (long)JULIAN_EPOCH_DAY) * TimeUnit.DAYS.toMillis(1);

        return (parquetDate - (long) PARQUET_EPOCH_DAY) * TimeUnit.DAYS.toMillis(1);
    }

    public static int toParquetDate(LocalDateTime localDateTime) {
//        return localDateTime.atOffset(ZoneOffset.UTC).get(ChronoField.EPOCH_DAY) + PARQUET_EPOCH_DAY;
        return (int) (localDateTime.atZone(ZoneOffset.systemDefault()).getLong(ChronoField.EPOCH_DAY) + PARQUET_EPOCH_DAY);
    }

    public static int toParquetDate(LocalDate localDate) {
        return (int) (localDate.getLong(ChronoField.EPOCH_DAY) + PARQUET_EPOCH_DAY);
    }

    public static LocalDateTime parquetDateToLocalDateTime(int parquetDate) {
        long epochTimeMillis = parquetDateToMillis(parquetDate);
        Instant instant = Instant.ofEpochMilli(epochTimeMillis);
        return instant.atOffset(ZoneOffset.UTC).toLocalDateTime();
    }

    public static int toParquetDate(Date date) {

        return (int)(date.getTime()/TimeUnit.DAYS.toMillis(1)) + PARQUET_EPOCH_DAY;
    }
}

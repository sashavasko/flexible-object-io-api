package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ParquetUtils {
    public static final int JULIAN_EPOCH_DAY = 4881176;
    public static final int EPOCH_DAY = 0; //Apache Drill prior to v 1.9 used Julian Date instead of proper UNix epoch 4881176;
    private static final long TIME_ZONE_OFFSET = new MutableDateTime(0).getZone().getOffset(0);
    private static final MutableDateTime EPOCH = new MutableDateTime(-TIME_ZONE_OFFSET);

    static final BiFunction<Comparable, Comparable, Comparable> calculateMax = (x1, x2) -> x1 == null || x2.compareTo(x1) > 0 ? x2 : x1;
    static final BiFunction<Comparable, Comparable, Comparable> calculateMin = (x1, x2) -> x1 == null || x2.compareTo(x1) < 0 ? x2 : x1;
    static final Function<Statistics, Comparable> getMax = (stats)->stats.genericGetMax();
    static final Function<Statistics, Comparable> getMin = (stats)->stats.genericGetMin();
    static final Function<Statistics, Comparable> getNumNulls = (stats)->stats.getNumNulls();

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

        int columnIdx = schema.getFieldIndex(column);

        List<BlockMetaData> rowGroups = footer.getBlocks();
        for (int index = 0; index < rowGroups.size(); index += 1) {
            Comparable blockMaxValue = getter.apply(rowGroups.get(index).getColumns().get(columnIdx).getStatistics());
            result = calculator.apply (result, blockMaxValue);
        }
        return result;
    }

    public static Comparable getMaxValueInFiles(Configuration conf, Path path, boolean recursive, String column) throws IOException {
        return getValueInFiles(conf, path, recursive, column, getMax, calculateMax);
    }

    public static Comparable getMinValueInFiles(Configuration conf, Path path, boolean recursive, String column) throws IOException {
        return getValueInFiles(conf, path, recursive, column, getMin, calculateMin);
    }

    public static Comparable getValueInFiles(Configuration conf, Path path, boolean recursive, String column, Function<Statistics, Comparable> getter, BiFunction<Comparable, Comparable, Comparable> calculator) throws IOException {
        RemoteIterator<LocatedFileStatus> iter = path.getFileSystem(conf).listFiles(path, recursive);
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

    public static int jodaDateToParquetDate (MutableDateTime vlcdate) {
        long millis = vlcdate.getMillis() - EPOCH.getMillis();
        int parquetDate = Days.daysBetween(EPOCH, vlcdate).getDays() + EPOCH_DAY;
        return parquetDate;
    }

    public static int jodaDateToParquetDate (DateTime dateTime) {
        long millis = dateTime.getMillis() - EPOCH.getMillis();
        int parquetDate = Days.daysBetween(EPOCH, dateTime).getDays() + EPOCH_DAY;
        return parquetDate;
    }

    public static long parquetDateToMillis(int parquetDate){
        if (parquetDate > JULIAN_EPOCH_DAY)
            return (parquetDate - (long)JULIAN_EPOCH_DAY) * DateTimeConstants.MILLIS_PER_DAY;

        return (parquetDate - (long)EPOCH_DAY) * DateTimeConstants.MILLIS_PER_DAY;
    }

    public static MutableDateTime parquetDateToMutableDateTime (int parquetDate){
        long millis = parquetDateToMillis(parquetDate);
        long timezoneMillis = millis - TIME_ZONE_OFFSET;
//        System.out.println("timezone millis out:" + timezoneMillis);
        return new MutableDateTime(new DateTime(timezoneMillis).withTimeAtStartOfDay());
    }

    public static DateTime parquetDateToDateTime (int parquetDate){
        long millis = parquetDateToMillis(parquetDate);

        long timezoneMillis = millis - TIME_ZONE_OFFSET;
//        System.out.println("timezone millis out:" + timezoneMillis);
        return new DateTime(new DateTime(timezoneMillis).withTimeAtStartOfDay());
    }
}

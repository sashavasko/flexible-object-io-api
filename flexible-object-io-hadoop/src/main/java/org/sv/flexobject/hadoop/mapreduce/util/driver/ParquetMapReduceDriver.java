package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetFilterParser;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;

import java.io.IOException;

public abstract class ParquetMapReduceDriver<SELF extends ParquetMapReduceDriver> extends MapReduceDriver<SELF> {
    Logger logger = Logger.getLogger(ParquetMapReduceDriver.class);

    public static final String PARQUET_FILTER_PREDICATE = "mapreduce.parquet.filter.predicate.json";

    Class<? extends StreamableWithSchema> inputSchemaClass;
    String inputSchemaJson;
    Class<? extends StreamableWithSchema> outputSchemaClass;
    String outputSchemaJson;
    boolean useFileSchema = false;

    public SELF setInputSchemaClass(Class<? extends StreamableWithSchema> inputSchemaClass) {
        this.inputSchemaClass = inputSchemaClass;
        return (SELF) this;
    }

    public SELF setInputSchemaJson(String inputSchemaJson) {
        this.inputSchemaJson = inputSchemaJson;
        return (SELF) this;
    }

    public SELF setOutputSchemaClass(Class<? extends StreamableWithSchema> outputSchema) {
        this.outputSchemaClass = outputSchema;
        return (SELF) this;
    }

    public SELF setOutputSchemaJson(String outputSchema) {
        this.outputSchemaJson = outputSchema;
        return (SELF) this;
    }

    public SELF setUseFileSchema() {
        this.useFileSchema = true;
        return (SELF) this;
    }

    public static void setParquetPredicate(Logger logger, Configuration config){
        String parquetPredicateJson = config.get(PARQUET_FILTER_PREDICATE);
        if (StringUtils.isNotEmpty(parquetPredicateJson)){
            try {
                FilterPredicate predicate = ParquetFilterParser.parse(parquetPredicateJson);
                logger.info("Using Parquet predicate filter:" + predicate.toString());
                ParquetInputFormat.setFilterPredicate(config, predicate);
            } catch (IOException e) {
                logger.error("Failed to parse Parquet predicate \"" + parquetPredicateJson + "\"", e);
            }
        }

    }

    @Override
    public void preConfigureJob() {
        Configuration conf = getConfiguration();
        ParquetSchemaConf parquetConf = new ParquetSchemaConf().from(conf);

        if (!parquetConf.hasInputSchema()){
            if (inputSchemaClass != null) {
                parquetConf.setInputSchemaClass(inputSchemaClass);
                logger.info("Set input parquet schema to " + inputSchemaClass.getName());
            } else if (inputSchemaJson != null) {
                try {
                    parquetConf.setInputSchemaJson(inputSchemaJson);
                    logger.info("Set input parquet schema to \"" + inputSchemaJson +"\"");
                } catch (IOException e) {
                    logger.error("Failed to parse input schema JSON: \"" + inputSchemaJson + "\"");
                }
            }
        }

        if (parquetConf.hasInputSchema() || useFileSchema) {
            setInputFormatClass(parquetConf.getInputFormat());
            setParquetPredicate(logger, conf);
        }

        if (!parquetConf.hasOutputSchema()){
            if (outputSchemaClass != null) {
                parquetConf.setOutputSchemaClass(outputSchemaClass);
                logger.info("Set output parquet schema to " + outputSchemaClass.getName());
            } else if (outputSchemaJson != null) {
                try {
                    parquetConf.setOutputSchemaJson(outputSchemaJson);
                    logger.info("Set output parquet schema to \"" + outputSchemaJson +"\"");
                } catch (IOException e) {
                    logger.error("Failed to parse output schema JSON: \"" + outputSchemaJson + "\"");
                }
            }
        }

        if (parquetConf.hasOutputSchema()){
            setOutput(parquetConf.getOutputFormat(), Void.class, parquetConf.getOutputClass());
            ParquetOutputFormat.setCompression(job, CompressionCodecName.SNAPPY);
            logger.info("Enabled snappy compression for output parquet format");
        }

        try {
            parquetConf.update(conf);
        } catch (Exception e) {
            logger.error("Failed to commit changes to parquet configuration", e);
        }
    }
}

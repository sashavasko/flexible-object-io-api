package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public abstract class OutputProducer<SELF extends OutputProducer> extends Configured implements Tool {
    Logger logger = Logger.getLogger(OutputProducer.class);

    protected Job job = null;
    protected Path outputPath = null;
    protected String[] cleanArgs = null;
    protected boolean skipTrash = false;

    public Job getJob() {
        return job;
    }

    public Path getOutputPath() {
        return outputPath;
    }

    public SELF prepareConfiguration(String[] args)
            throws Exception {

        logger.info("Preparing configuration. Args are : " + StringUtils.join(args, '|'));
        Configuration conf = getConf();
        if (conf == null)
            conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        setConf(conf);
        cleanArgs = parser.getRemainingArgs();
        logger.info("Parsed generic hadoop options. Remaining Args are : " + StringUtils.join(cleanArgs, '|'));
        return (SELF) this;
    }

    public Options setupStandardOptions(){
        Options options = new Options();
        options.addOption("s", "skipTrash", false, "permanently delete the output directory (destructive action)");
        options.addOption("i", "inputFolder", true, "comma separated list of the path to the input folders (a folder in HDFS)");
        options.addOption("o", "outputFolder", true, "the path to the output folder in HDFS");
        options.addOption("h", "help", false, "print this document");

        return options;
    }

    protected CommandLine parseStandardOptions(Options options, String[] args) throws ParseException, IllegalArgumentException, IOException {
        boolean throwIllegalArguemntException = false;
        String errorMessageToDisplay = "";
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args, true);

        setSkipTrash(cmd.hasOption("s"));

        if (cmd.hasOption("i")) {
            defineInputs(Arrays.asList(cmd.getOptionValue("i").split(",")));
        } else {
            errorMessageToDisplay += "input folder needed\n";
            throwIllegalArguemntException = true;
        }
        if (cmd.hasOption("o")) {
            defineOutput(new Path(cmd.getOptionValue("o")));
        } else {
            errorMessageToDisplay += "output folder needed\n";
            throwIllegalArguemntException = true;
        }

        if(cmd.getOptions().length == 0 || cmd.hasOption("h") || throwIllegalArguemntException){
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.setWidth(120);
            helpFormatter.printHelp("hadoop jar <DriverClass> [<STANDARD HADOOP OPTIONS>] -i <inputDir1[,inputDir2]...> -o <outputDir> [-s] [-conf jobConfig.xml] \n", options);
            System.out.println("Where <STANDARD HADOOP OPTIONS> : ");
            GenericOptionsParser.printGenericCommandUsage(System.out);
            System.out.println("\n" + errorMessageToDisplay );
            return null;
        }

        return cmd;
    }


    public SELF prepareJob(String[] args) throws Exception {
        prepareConfiguration(args);
        logger.info("Preparing the job for running using " + getConf());

        Configuration config = getConf();
        Limits.init(config);

        if (job == null) {
            job = Job.getInstance(config);

            if (config.get("mapreduce.job.jar") == null)
                job.setJarByClass(this.getClass());

            if (StringUtils.isEmpty(job.getJobName()))
                job.setJobName(getJobName());
            logger.info("Driver setup with " + getConf());
            logger.info("Job    setup with " + job.getConfiguration());
        }
        return (SELF) this;
    }

    public SELF defineInput(Path input) throws IOException {
//        FileSystem fs = getHDFS();
//        if (fs.exists(input)) {
            logger.info("Adding " + input.toString() + " to Map Reduce");
            FileInputFormat.addInputPath(job, input);
//        }
//        else{
//            System.out.println(">>>>>>>>>>> " + input.toString() + " does not exist");
//        }
        return (SELF) this;
    }

    public SELF defineInputs(List<String> inputs) throws IOException {
        for (String s : inputs) {
            defineInput(new Path(checkPathForWildCard(s)));
        }
        return (SELF) this;
    }

    public static String checkPathForWildCard(String s){
        if(s.contains(".")){
            return s;
        }
        return s.endsWith("*") ? s : s.endsWith("/") ? s + "*" : s + "/*";
    }

    public SELF defineOutput(Path output){
        outputPath = output;
        logger.info("Writing output to: " + output);
        FileOutputFormat.setOutputPath(job, outputPath);
        return (SELF) this;
    }

    public void defineOutput(String output) throws IOException {
        defineOutput(new Path(output));
    }

    private void deletePath(FileSystem fs, Path toDelete) throws IOException {
        if (skipTrash)
            fs.delete(toDelete, true);
        else
            Trash.moveToAppropriateTrash(fs, toDelete, getConfiguration());
    }

    protected void defineOutputAsInput(String output) throws IOException {
        FileSystem fs = getHDFS();
        Path outputPath = new Path(output);
        if (fs.exists(outputPath)) {
            Path oldOutputPath = new Path(output + "-previous");
            if (fs.exists(oldOutputPath)) {
                logger.info("Deleting old " + oldOutputPath.toString());
                deletePath(fs, oldOutputPath);
            }

            logger.info("Renaming " + outputPath.toString() + " to " + oldOutputPath.toString());
            fs.rename(outputPath, oldOutputPath);

            Path lastInputPath = new Path(output + "-previous/*.avro");
            defineInput(lastInputPath);
        }else {
            throw (new RuntimeException("The last in " + outputPath.toString() + " does not exist. Aborting!"));
        }

        defineOutput(outputPath);
    }


    public FileSystem getHDFS() throws IOException {
        return FileSystem.get(getConfiguration());
    }

    public Configuration getConfiguration() {
        if (job != null)
            return job.getConfiguration();
        return getConf();
    }

    public void cleanupOutput() throws IOException {
        FileSystem fs = getHDFS();
        if (fs.exists(outputPath)) {
            logger.info("Output path " + outputPath + " exists, will be deleted.");
            deletePath(fs, outputPath);
        }
    }

    public void setSkipTrash(boolean skipTrash) {
        this.skipTrash = skipTrash;
    }

    protected abstract String getJobName();

    public void setQueueName(String queueName) {
        job.getConfiguration().set("mapreduce.job.queuename",  queueName);
    }

    protected void enableMapperOutputSnappyCompression() {
        Configuration config = getConfiguration();
        config.setBoolean("mapreduce.map.output.compress", true);
        config.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    }

    protected void enableUberMode() {
        Configuration config = getConfiguration();
        config.setBoolean("mapreduce.job.ubertask.enable", true);
        config.setBoolean("mapreduce.map.output.compress", false);
        config.setInt("mapreduce.job.ubertask.maxbytes", 9 * 128 * 1024 * 1024);
        config.setInt("yarn.app.mapreduce.am.resource.mb", 9216);
    }

    protected SELF setOutputToNoOutput(){
        if (job != null)
            job.setOutputFormatClass(NoOutputFormat.class);
        return (SELF) this;
    }

    protected void writeOutCount(FSDataOutputStream os, CounterGroup group, String startsWith) throws IOException {
        for(Counter counter : group){
            if(startsWith == null || counter.getDisplayName().startsWith(startsWith))
                os.writeBytes(counter.getDisplayName() + "\t" + counter.getValue() + "\n");
        }
    }

}

package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class ConfiguredDriver<SELF extends ConfiguredDriver> extends OutputProducer<SELF> {

    String jobSubmitResult = null;

    Logger logger = Logger.getLogger(OutputProducer.class);
    public abstract void configureJob() throws Exception;

    @Override
    protected String getJobName() {
        return job.getJobName();
    }


    @Override
    public SELF prepareJob(String[] args) throws Exception {
        super.prepareJob(args);
        if (!parseOptions (args))
            return null;

        cleanupOutput();
        return (SELF) this;
    }

    @Override
    public int run(String[] args) throws Exception {
        if (job == null) {
            if (prepareJob(args) == null)
                return 1;
        }

        configureJob();

        if (LookupsManager.isConfigured(this))
            LookupsManager.cacheLookups(this);

        logger.info("Starting the Map Reduce job :" + getJobName());
        int status;
        if (jobSubmitResult != null)
            status = "success".equals(jobSubmitResult) ? 0 : 1;
        else
            status = job.waitForCompletion(true) ? 0 : 1;

        if (status == 0){
            logger.info("Job succeeded.");
            handleJobSuccess();
        }else{
            logger.error("Job failed.");
            handleJobFailure();
        }

        writeOutCustomCounters();
        job = null;
        return status;
    }

    protected boolean parseOptions(String[] args) {
        Options options = setupStandardOptions();
        options.addOption("skipJobSubmit", true, "skip job submission and progress monitoring. Useful for debugging of startup process. Values are success and fail.");

        boolean result;

        try {
            CommandLine cmd = parseStandardOptions(options, args);
            if (cmd != null && cmd.hasOption("skipJobSubmit")){
                jobSubmitResult = cmd.getOptionValue("skipJobSubmit");
            }
            result = (cmd != null);
        } catch (ParseException e) {
            logger.error("Failed to parse options", e);
            return false;
        } catch (IOException e) {
            logger.error(e);
            return false;
        }

        return result;
    }

    protected void handleJobFailure() {

    }

    protected void handleJobSuccess() {

    }

    protected void writeOutCustomCounters() throws IOException {

    }
}

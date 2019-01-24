package ru.mephi.hw1.skok;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Program which counts average bytes per request by IP and total bytes by IP
 * Output Format is CSV file
 * Requirements are:
 * 1. Counters is used for statistics about malformed rows collection
 * 2. Writable entities are reused
 */

public class Driver extends Configured implements Tool {

    /**
     * Main logic: make configuration and job to count bytes in file,
     * set parameters of Mapper, Reducer, Input/Output data,
     * check for existing same output file and if true delete it (instead of manual removal)
     *
     * @param -> input/output directories (path)
     */
    public static void main(String[] args) throws Exception {
        int driverResult = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(driverResult);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        // Number of arguments must be 2: input and output directories
        if (args.length != 2) {
            System.err.println("Wrong usage: <inputDirectory> <outputDirectory>");
            System.exit(2);
        }

        // Output format is CSV that's why separator must be added
        configuration.set("mapreduce.output.textoutputformat.separator", ",");

        // Make new Job to count bytes
        Job job = Job.getInstance(configuration, "ByteCounter");
        job.setJarByClass(getClass());

        // Parameters of Mapper
        job.setMapperClass(BytesMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // Parameters of Reducer
        job.setReducerClass(BytesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input data parameters
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Output data parameters
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Check for same output file and delete it if true
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(args[1])))
            fs.delete(new Path(args[1]), true);

        // Completion job status
        int jobResult = job.waitForCompletion(true) ? 0 : 1;
        return jobResult;
    }
}

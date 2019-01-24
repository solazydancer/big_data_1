package ru.mephi.hw1.skok;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BytesMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * Writable entities are reused -> textForIP (Text), longForBytes (LongWritable)
     * Main logic: textForIP is used for IP address, longForBytes is used for amount of bytes per request
     **/

     private Text textForIP = new Text();
     private LongWritable longForBytes = new LongWritable(0);

    /**
     * Main logic: for each log string -> check for validation,
     * use result for statistics (count++), write to context parsed IP and amount of bytes
     * Output: IP, amount of bytes
     * @param -> key (log string number), value (log string), context MR
     */

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String logString = value.toString();
        try{
            checkValidation(logString);
            context.write(getIP(logString), getBytes(logString));
            context.getCounter(Log.class.getName(), Log.VALIDLOG.name()).increment(1);
        } catch (Exception e) {
            context.getCounter(Log.class.getName(), Log.INVALIDLOG.name()).increment(1);
        }
    }

    /**
     * Main logic: default flag is false, if line contains IP (sign is - -) and is not empty -> flag becomes true
     * @param -> line (log string)
     * @return -> true if line contains IP and is not empty (contains amount of bytes)
     */

    private boolean checkValidation(String line) {
        boolean flag = false;
        boolean notEmpty;
        try {
            getBytes(line);
            notEmpty = true;
        } catch (Exception e) {
            notEmpty = false;
        }
        if ((line.contains(" - - "))  && notEmpty) flag = true;
        return flag;
    }

    /**
     * Main logic: IP is text from 0th position to sign - -
     * @param -> log string
     * @return -> IP
     */
    private Text getIP(String line) {
        textForIP.set(line.substring(0, line.indexOf(" - - ")));
        return textForIP;
    }

    /**
     * Main logic: amount of bytes is a number btw "HTTP"+14 (based on the experience of viewing input data)
     * and first space after "HTTP..."
     * @param -> log string
     * @return -> amount of bytes
     */
    private LongWritable getBytes(String line) {
        int i = line.indexOf("HTTP/1.0\" ") + 14;
        longForBytes.set(Long.parseLong(line.substring(
                i, line.indexOf(' ', i))));
        return longForBytes;
    }

}

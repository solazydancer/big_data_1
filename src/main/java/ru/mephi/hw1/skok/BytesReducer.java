package ru.mephi.hw1.skok;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesReducer extends Reducer<Text,LongWritable,Text,Text>  {
    /**
     * Main logic: for each IP (key) total and average bytes are calculated and written to context
     * Output: IP, average bytes per request, total bytes
     * @param -> key (IP), values (amount of bytes), context MR
     */

    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws InterruptedException, IOException {
        long bytesSum = 0;
        long count = 0;
        long average;
        for (LongWritable value : values) {
            bytesSum += value.get();
            count++;
        }
        average = bytesSum/count;
        context.write(key, new Text(average + "," + bytesSum));
    }
}

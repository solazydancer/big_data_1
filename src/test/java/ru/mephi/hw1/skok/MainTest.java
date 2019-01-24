package ru.mephi.hw1.skok;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class MainTest {

    /**
     * Main logic: initialize Mapper/Reducer/MR, tests instances for them, set log string for tests
     * Run 3 tests
     */

    MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
    ReduceDriver<Text,LongWritable,Text,Text> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, Text> mapReduceDriver;
    String testLog = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET " +
            "/~strabal/grease/photo9/927-3.jpg HTTP/1.0\" 200 40028 \"-\" \"Mozilla/5.0" +
            " (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";
    String testLog2 = "ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET " +
            "/~strabal/grease/photo9/927-3.jpg HTTP/1.0\" 200 8 \"-\" \"Mozilla/5.0" +
            " (compatible; YandexImages/3.0; +http://yandex.com/bots)\"";

    @Before
    public void setUp() {
        BytesMapper mapper = new BytesMapper();
        BytesReducer reducer = new BytesReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    /**
     * Test for Mapper
     * Input params: test log string
     * Output must be formatted as: ip1 (Text), amount of bytes per request (LongWritable)
     * @throws IOException
     */

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(testLog));
        mapDriver.withOutput(new Text("ip1"), new LongWritable(40028));
        mapDriver.runTest();
    }

    /**
     * Test for Reducer
     * Input params: amounts of bytes (ArrayList<LongWritable>)
     * Output must be formatted as: ip1 (Text), average + total amount of bytes (Text)
     * @throws IOException
     */

    @Test
    public void testReducer() throws IOException {
        List<LongWritable> values = new ArrayList<LongWritable>();
        values.add(new LongWritable(5012));
        values.add(new LongWritable(40020));
        reduceDriver.withInput(new Text("ip1"), values);
        reduceDriver.withOutput(new Text("ip1"), new Text("22516,45032"));
        reduceDriver.runTest();
    }

    /**
     * Test for MR
     * Input params: 2 test log strings
     * Output must be formatted as: ip1 (Text), average + total amount of bytes (Text)
     * @throws IOException
     */

    @Test
    public void testMR() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(testLog));
        mapReduceDriver.withInput(new LongWritable(), new Text(testLog2));
        mapReduceDriver.withOutput(new Text("ip1"), new Text("20018,40036"));
        mapReduceDriver.runTest();
    }
}

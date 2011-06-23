package com.dappervision.examples; // NOTE: This was taken verbatim from the hadoop examples
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class WordCount {

public static class MyMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, IntWritable> {
      private final IntWritable one = new IntWritable(1);
      private Text word = new Text();
    
      public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          StringTokenizer itr = new StringTokenizer(value.toString());
          while (itr.hasMoreTokens()) {
              word.set(itr.nextToken());
              output.collect(word, one);
          }
      }
}


public static class MyReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
          sum += values.next().get();
      }
      result.set(sum);
      output.collect(key, result);
    }
}

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("Wordcount");
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        JobClient.runJob(conf);
    }
}

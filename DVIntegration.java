package com.dappervision.examples;
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
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.BytesWritable;
import java.util.TreeMap;
import org.apache.hadoop.typedbytes.TypedBytesWritable;
import static java.lang.System.out;


public class DVIntegration {

public static class MyMapper extends MapReduceBase
    implements Mapper<TypedBytesWritable, TypedBytesWritable, Text, IntWritable> {
    
      public void map(TypedBytesWritable key, TypedBytesWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
          out.println(((TreeMap)value.getValue()).get("image_data"));
          output.collect(new Text("Dummy"), new IntWritable(5)); // Output junk so that the reducer gets input
      }
}


public static class MyReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, MapWritable> {

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, MapWritable> output, Reporter reporter) throws IOException {
        MapWritable result = new MapWritable();
        result.put(new Text("extension"), new Text("mp4"));
        result.put(new Text("video_data"), new Text("reallyreallylongbinarystringit'ssolong oh yeah it can have spaces"));
        result.put(new Text("hdfs_path"), new Text("/user/brandyn/videos/hugevideo"));
        output.collect(key, result);
    }
}

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: DVIntegration <input path> <output path>");
            System.exit(-1);
        }
        JobConf conf = new JobConf(DVIntegration.class);
        conf.setJobName("DVIntegration");
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(MyMapper.class);
        conf.setReducerClass(MyReducer.class);
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(MapWritable.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        JobClient.runJob(conf);
    }
}

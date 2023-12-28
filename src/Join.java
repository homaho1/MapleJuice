import java.io.IOException;
import java.io.ObjectInputFilter.Config;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.*;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.*;

public class Join {

  private static String regex;
  public static class JoinMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    String regex = conf.get("regex");
        String dataset = conf.get("dataset");
        int start_idx = regex.indexOf(dataset) + dataset.length() + 1;
        int end_idx = regex.substring(start_idx).indexOf(' ');
        String field_name = regex.substring(start_idx, end_idx);
	    // System.out.println(PercentComp.regex);
        Matcher m = Pattern.compile(field_name + "\": \"(.*?)\"").matcher(value.toString());
        if (m.find()) {
            String field = m.group(1);
            String v = dataset + ", " + value.toString();
            context.write(field, new Text(v));
        }
    }
}

  public static class JoinReducer
       extends Reducer<Text, Text, NullWritable, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
        
        HashSet<String> map = new HashSet<String>();
        for (Text val : values) {
            String[] fields = val.toString().split(", ");
            String dataset = fields[0];
            String value = fields[1];
            map.add(dataset);
        }
        if (map.size() == 2) {
            for (Text val : values) {
                String[] fields = val.toString().split(", ");
                String dataset = fields[0];
                String value = fields[1];
                context.write(NullWritable.get(), new Text(value));
            }
        }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    conf.set("regex", args[0]);
    conf.set("dataset", args[1])
    Job job1 = Job.getInstance(conf, "join1");
    // Set Mapper, no Reducer
    job1.setMapperClass(JoinMapper.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job1, new Path(args[1]));
    FileOutputFormat.setOutputPath(job1, new Path("intermediate_output1"));

    // Wait for Job 1 to complete
    job1.waitForCompletion(true);

    // Job 2 Configuration
    Configuration conf2 = new Configuration();
    Job job2 = new Job(conf2, "Job 2");
    // Set Mapper, Reducer
    conf2.set("dataset", args[2])
    job2.setMapperClass(JoinMapper.class);
    job2.setReducerClass(JoinReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(args[2]));
    FileInputFormat.addInputPath(job2, new Path("intermediate_output1")); // Input from Job 1
    FileOutputFormat.setOutputPath(job2, new Path(args[3]));

    job2.waitForCompletion(true);
  }
}

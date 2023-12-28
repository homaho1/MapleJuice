import java.io.IOException;
import java.util.HashMap;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class PercentComp {

  private static String regex;
  public static class RegexMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
	    Configuration conf = context.getConfiguration();
	    String regex = conf.get("regex");
	    // System.out.println(PercentComp.regex);
      	    Matcher m = Pattern.compile(regex).matcher(value.toString());
            if (m.find()) {
                // extract the value of Detection_ field
                Matcher m2 = Pattern.compile("Detection_\": \"(.*?)\"").matcher(value.toString());
                if (m2.find()) {
                    String detection = m2.group(1);
                    context.write(one, new Text(detection));
                }
            }
    }
  }

  public static class PercentCompReducer
       extends Reducer<IntWritable, Text, Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      HashMap <String, Integer> hmap = new HashMap<String, Integer>();
      double count = 0;
      for (Text val : values) {
        count += 1;
        if (hmap.containsKey(val.toString())) {
            hmap.put(val.toString(), hmap.get(val.toString()) + 1);
        } else {
            hmap.put(val.toString(), 1);
        }
      }
      for (String k : hmap.keySet()) {
        result.set(hmap.get(k) / count);
        System.out.println(k);
        System.out.println(k.getClass().getName());
        System.out.println(result.getClass().getName());
        context.write(new Text(k), result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("regex", args[0]);
    Job job = Job.getInstance(conf, "percent comp");
    job.setJarByClass(PercentComp.class);
    job.setMapperClass(RegexMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    // job.setCombinerClass(PercentCompReducer.class);
    job.setReducerClass(PercentCompReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    PercentComp.regex = args[0];
    System.out.println(PercentComp.regex);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

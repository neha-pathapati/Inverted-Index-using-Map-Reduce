import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InvertedIndex {
   
   public static class InvertedIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
   
    private final static Text word = new Text();
    private final static Text filename = new Text();
   
    public void map(LongWritable key, Text val,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
      String file_name = fileSplit.getPath().getName().split("\\.")[0];
      filename.set(file_name);
      StringTokenizer itr = new StringTokenizer(val.toString());

      while(itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, filename);
      }
    }
  }
  
  public static class InvertedIndexReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
      HashMap<String, Integer> hm = new HashMap<String, Integer>();
      while(values.hasNext()) {
         String val = values.next().toString();
         Integer count = hm.get(val);
          if (count == null) {
             count = 0;
          }
          hm.put(val, count + 1);
      }
      boolean first = true;
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, Integer> entry : hm.entrySet()) {
        if (!first) {
          sb.append("\t ");
        }
        first=false;
        sb.append(entry.getKey()).append(":").append(entry.getValue());
      }
      output.collect(key, new Text(sb.toString()));
    }
  }
  
  public static void main(String[] args) {
    JobClient client = new JobClient();
    JobConf conf = new JobConf(InvertedIndex.class);
    conf.setJobName("InvertedIndexer");
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setMapperClass(InvertedIndexMapper.class);
    conf.setReducerClass(InvertedIndexReducer.class);
    client.setConf(conf);
    try {
        JobClient.runJob(conf);
    }
    catch(Exception e) {
        e.printStackTrace();
    }
  }
}
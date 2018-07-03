import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InvertedIndex {

        public static class InvertedMapper extends Mapper<Object, Text, Text, Text>{
                //private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();
                private Text docId = new Text();
                public void map(Object key, Text value, Context context
                                ) throws IOException, InterruptedException {
                        StringTokenizer iterator = new StringTokenizer(value.toString());
                        docId.set(iterator.nextToken());
                        while (iterator.hasMoreTokens()) {
                                word.set(iterator.nextToken());
                                context.write(word, docId);
                        }
                }
        }
        public static class InvertedReducer extends Reducer<Text,Text,Text,Text> {
              
                private Text map1 = new Text();
               
                public void reduce(Text key, Iterable<Text> values,Context context
                                ) throws IOException, InterruptedException {
                        HashMap<String, Integer> hashmap = new HashMap<String, Integer>();
                        for (Text val : values) {
                                if(hashmap.containsKey(val.toString())){
                                hashmap.put(val.toString(),hashmap.get(val.toString())+1);
                                }else{
                                hashmap.put(val.toString(),1);
                                }
                        }
                        StringBuilder s = new StringBuilder();
                        for(Map.Entry<String, Integer> pair : hashmap.entrySet()){
                                s.append(pair.getKey());
                                s.append(":");
                                s.append(pair.getValue());
                                s.append("\t");
                        }
                        map1.set(s.toString());
                        context.write(key, map1);
                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job1 = Job.getInstance(conf, "word count");
                job1.setJarByClass(InvertedIndex.class);
                job1.setMapperClass(InvertedMapper.class);
                job1.setReducerClass(InvertedReducer.class);
                job1.setOutputKeyClass(Text.class);
                job1.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job1, new Path(args[0]));
                FileOutputFormat.setOutputPath(job1, new Path(args[1]));
                System.exit(job1.waitForCompletion(true) ? 0 : 1);
                }
}

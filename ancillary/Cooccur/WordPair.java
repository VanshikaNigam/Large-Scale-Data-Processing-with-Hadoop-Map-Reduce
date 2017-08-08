import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordPair {

public static class PairCount
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


      //String [] each_line=value.toString().split("line.separator");
      //for(int line=0;line<each_line.length;line++){

            //String [] word_token=each_line[line].toString().split("\\s+");
      String[] word_token = value.toString().split("\\s+"); 
      
      if (word_token.length > 1) {
          for (int i = 0; i < word_token.length; i++) {
            //if(!word_token[i].equals(" ")){
              //wordPair.setWord(tokens[i]);

             //int start = (i - word_token.length < 0) ? 0 : i - word_token.length;
             //int end = (i + word_token.length >= tokens.length) ? tokens.length - 1 : i + neighbors;
              for (int j = 0; j < word_token.length; j++) {
                  if (j == i) continue;

                  word.set(word_token[i]+","+word_token[j]);
                    //System.out.print("PRINT STATEMENT:"+word_token[i]+word_token[j]); 
                   context.write(word, one);
                 }
              }
          }
      }
      
    }
 
  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word pair");
    job.setJarByClass(WordPair.class);
    job.setMapperClass(PairCount.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
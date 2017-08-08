import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Set;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordStripe {

  public static class MyMap extends MapWritable{
    public String toString(){
      StringBuilder res=new StringBuilder();
      Set <Writable> keySet=this.keySet();
      for (Object key :keySet ) {

          res.append("["+key.toString()+"="+this.get(key)+"]");        
      }
      return res.toString();
    }
  }

public static class Stripes
       extends Mapper<Object, Text, Text, MyMap>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private MyMap occur_count= new MyMap();
    IntWritable count;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      //String clean_value[]=value.toString().split(">");
     	//clean_value[1]=clean_value[1].trim();
      //clean_value[1]=clean_value[1].replaceAll("\\p{P}","");

      //String[] word_token = clean_value[1].toString().split("\\s+"); 
    	String[] word_token=value.toString().split("\\s+");
      
      if (word_token.length > 1) {
          for (int i = 0; i < word_token.length; i++) {
              word.set(word_token[i]);
              occur_count.clear();
          
              for (int j = 0; j < word_token.length; j++) {
                  if (j == i) continue;
                  else
                  {
                    Text next_token = new Text(word_token[j]);
                if(occur_count.containsKey(next_token)){
                   count = (IntWritable)occur_count.get(next_token);
                   count.set(count.get()+1);
                }else{
                   occur_count.put(next_token,new IntWritable(1));
                }
                  }
                }
                    //word.set(word_token[i]+","+word_token[j]);
                    //System.out.print("PRINT STATEMENT:"+word_token[i]+word_token[j]); 
                   context.write(word, occur_count);
                 }
              }
          }
      }
      
 
  public static class StripesReducer
       extends Reducer<Text,MapWritable,Text,MyMap> {
    private MyMap result = new MyMap();
         // private MapWritable result=new MapWritable();
    public void reduce(Text key, Iterable<MapWritable> values,Context context) throws IOException, InterruptedException {
      
      result.clear();
      for (MapWritable i:values)
      {
        Set<Writable> keys = i.keySet();
        for (Writable j : keys) {
            IntWritable fromCount = (IntWritable) i.get(j);
            if (result.containsKey(j)) {
                IntWritable count = (IntWritable) result.get(j);
                count.set(count.get() + fromCount.get());
            } else {
                result.put(j, fromCount);
            }
        }

      }
      context.write(key,result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word stripes");
    job.setJarByClass(WordStripe.class);
    job.setMapperClass(Stripes.class);
    job.setCombinerClass(StripesReducer.class);
    job.setReducerClass(StripesReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MyMap.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
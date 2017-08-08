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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Arrays;


public class Counting {
 

static FileReader fileread;
static BufferedReader buffread;
static HashMap<String, String> lemma= new HashMap<String,String>();

static String FILE_NAME = "/home/hadoop/Downloads/new_lemmatizer.csv";
  //br= new BufferedReader(new FileReader(FILENAME));

public static void readlemma()
  {
    try{
    fileread= new FileReader(FILE_NAME);
     buffread = new BufferedReader(fileread); 
     String lemma_line="";
     while((lemma_line=buffread.readLine())!=null)
     {

          lemma_line=lemma_line.replaceAll(","," ");
          //System.out.println(line);
         //String t[]=line.split(" ");
        //System.out.println("size="+t.length);
          List<String> lemma_list = Arrays.asList(lemma_line.split(" "));

          //List<String> items = Arrays.asList(sCurrentLine.split("\\s*,\\s*"));
          String str_list="";
          for(int i=1;i<lemma_list.size();i++)
          {
           str_list=str_list+lemma_list.get(i)+ " ";
        }
        str_list=str_list.trim();
        lemma.put(lemma_list.get(0), str_list);
     }
  }
  catch(IOException e)
  {
    e.printStackTrace();
  }
}

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text location= new Text();
    //ArrayList<String> arr= new ArrayList<String>();

     //String FILENAME = "/home/hadoop/Downloads/new_lemmatizer.csv";
        //FileReader fr=new FileReader(FILENAME);
        //BufferedReader br = new BufferedReader(fr);  

     public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //System.out.println(inputStream.readChar());

      String tess_store[]=value.toString().split("\\t");
      location.set(tess_store[0]);
      //System.out.print("Location"+rec[0]);
      //System.out.print("text"+rec[1]);
     tess_store[1]=tess_store[1].trim().replaceAll("\\p{P}","");
      String[] word_token = tess_store[1].toString().split("\\s+"); 

      //StringTokenizer itr = new StringTokenizer(rec[1]);
      for (int i = 0; i < word_token.length; i++){
        word.set(word_token[i]);
       String norm=word.toString().toLowerCase();
       //System.out.println("case change"+ norm);
       String clean_1=norm.toString().replaceAll("j","i");
       //System.out.println("change j to i"+ clean_1);
       String clean_2=clean_1.toString().replaceAll("v","u");
       //System.out.println("change v to u"+ clean_2);

       Text word_final=new Text(clean_2);
        //context.write(word, one); //REPLACE I WITH U
      //System.out.println("reached here");
      //}
       String eachLine;

    if(lemma.containsKey(clean_2))
    {
       List<String> l = Arrays.asList(lemma.get(clean_2).split(" "));
       for(String st: l)
       {
        Text latin=new Text(st);
        context.write(latin,location);

       }
    }
    context.write(word_final,location);
  }
  }
}

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text count_text = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String combine = "";
          for (Text txt : values) {
              combine += " " + txt.toString();
              //translations=translations.replaceAll("\t","");

          }
          count_text.set(combine);

          context.write(key,count_text);

      }
  }
    


  public static void main(String[] args) throws Exception {
    


    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Word_Lemma_Count");
    job.setJarByClass(Counting.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setNumMapTasks(5);
    //job.setNumReduceTasks(2);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    readlemma();

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

  
}


import java.io.IOException;
import java.lang.String;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author SPARSH
 */
public class PaymentInfo {
    static class MyMapper extends Mapper<Object,Text,Text,IntWritable>
    {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
              String s=value.toString();
              String[] arr=s.split(",");
           try{
              String a=arr[2];
              String c=arr[3];
              int x=Integer.parseInt(a);
               context.write(new Text(c), new IntWritable(x));
            }catch(Exception ee)
            {
             System.out.println(ee);
            }
        }
    }
    static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val:values){
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
            
        }
        
    }
    public static void main(String[] args) throws Exception
    {
      Job j=new Job(new Configuration(),"PI");  
      j.setJarByClass(PaymentInfo.class);
      j.setMapperClass(MyMapper.class);
      j.setCombinerClass(MyReducer.class);
      j.setReducerClass(MyReducer.class);
      j.setMapOutputKeyClass(Text.class);
      j.setMapOutputValueClass(IntWritable.class);
      j.setMapOutputKeyClass(Text.class);
      j.setOutputValueClass(IntWritable.class);
      Path pin=new Path(args[0]);
      Path pout=new Path(args[1]);
      FileInputFormat.addInputPath(j, pin);
      FileOutputFormat.setOutputPath(j, pout);
      System.exit(j.waitForCompletion(true)?0:1);
      
    }
    
}

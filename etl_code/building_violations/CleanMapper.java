
import java.io.IOException;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author DELL
 */


import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class CleanMapper extends Mapper<LongWritable, Text, Text,Text>{
    
    int counter = 0 ;
     
    @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // TODO code application logic here
         String input[]  = value.toString().split("\"");
         
         String rest[] =  input[0].split(",");
         String output = "";
         for(int i = 0 ; i<rest.length ; i++)
         {
             if(i== 8)
             {
                 continue;
             }
           output = output+rest[i];
         }
         
         for(int i = 1 ; i<input.length;i++)
         {
             output = output+input[i];
         }
         
         context.write(new Text(""+(counter++)), new Text(output));
        }
}

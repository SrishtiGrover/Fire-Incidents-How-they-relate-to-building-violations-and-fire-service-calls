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
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
class ProfReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    
    public void reduce(Text key, Iterable<IntWritable> values, Context context)throws IOException, InterruptedException
        {
            
            
                int max = -1;
                for (IntWritable value : values)
                {
                    max = Math.max(value.get(),max);  
                }
                
                 context.write(key, max);
        
        }
            
    }


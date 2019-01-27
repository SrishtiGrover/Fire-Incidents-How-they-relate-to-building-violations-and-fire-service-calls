import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanReducer
    extends Reducer<String, String, String, String> {
  
  @Override
  public void reduce(String key, Iterable<String> values, Context context)
      throws IOException, InterruptedException {
    
   
    
    ///////////////Reducer simply writes data on a new file- that will become new dataset after cleaning/////////////////////////////////////////////
    

      for (String value : values) 
      {
        context.write(key, new String(value));
      
      }
      
  


   }

}


               


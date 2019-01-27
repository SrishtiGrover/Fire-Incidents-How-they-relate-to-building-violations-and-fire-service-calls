import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
    extends Mapper<LongWritable, Text, String, String> {

  private static final int MISSING = 9999;
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String[] columnlist=line.split(",") ;
    int num_columns=columnlist.length;


    /// For all attributes that are needed, just send the (column name, value) directly as (key,value) pairs to the reducer/////
    /// the other columns will get dropped in this process ///////
    context.write(new String("Call_number"), new String(columnlist[0]));
    context.write(new String("Unit_ID"), new String(columnlist[1]));
    context.write(new String("Incident_Number"), new String(columnlist[2]));
    context.write(new String("Call_Type"), new String(columnlist[3]));
    context.write(new String("Call_Date"), new String(columnlist[4]));
    context.write(new String("Watch_Date"), new String(columnlist[5]));
    context.write(new String("Received_DtTm"), new String(columnlist[6]));
    context.write(new String("On_Scene_DtTm"), new String(columnlist[10]));
    context.write(new String("Transport_DtTm"), new String(columnlist[11]));
    context.write(new String("Hospital_DtTm"), new String(columnlist[12]));
    context.write(new String("Address"), new String(columnlist[15]));
    context.write(new String("City"), new String(columnlist[16]));
    context.write(new String("Zipcode_of_Incident"), new String(columnlist[17]));
    context.write(new String("Station_Area"), new String(columnlist[19]));
    context.write(new String("Priority"), new String(columnlist[22]));
    context.write(new String("Call_Type_Group"), new String(columnlist[25]));
    context.write(new String("Fire_Prevention_District"), new String(columnlist[29]));
    context.write(new String("Supervisor_District"), new String(columnlist[30]));
    context.write(new String("Neighborhood_Analysis_Boundaries"), new String(columnlist[31]));
    context.write(new String("Location"), new String(columnlist[32]));
    context.write(new String("RowID"), new String(columnlist[33]));
    
      
       

    
  }
}
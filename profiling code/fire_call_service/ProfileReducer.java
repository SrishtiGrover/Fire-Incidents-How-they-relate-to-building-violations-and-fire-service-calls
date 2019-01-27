import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProfileReducer
    extends Reducer<String, String, String, String> {
  
  @Override
  public void reduce(String key, Iterable<String> values, Context context)
      throws IOException, InterruptedException {
    
    ///////// finding min and max values for each numeric attribute //////////
    ///////// finding max length of all string attributes  //////////
    
    //////////////////////////////////////////////////////////////////////////////////
    int maxValue = Integer.MIN_VALUE;
    int minValue = Integer.MAX_VALUE;

    if (key== "Call_number")
    {
      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min));
  

    }
     ////////////////////////////////////////////////////////////////////////////////
    int l=0;
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Unit_ID")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Incident_Number")
    {
      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min));
  

    }
    
     

    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Call_Type")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Call_Date")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////


    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Watch_Date")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Received_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Entry_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////


    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Dispatch_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////


    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Response_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "On_Scene_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Transport_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Hospital_DtTm")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Call_final_Disposition")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Available_DtTm ")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Address")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "City")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Zipcode_of_Incident")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Battalion")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Station_Area")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Box")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Original_Priority")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Priority")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Final_Priority")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "ALS_Unit")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Call_Type_Group")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     

    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Number_of_Alarms")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Unit_Type")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Unit_sequence_in_call_dispatch")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Fire_Prevention_District")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Supervisor_District")
    { 

      for (String value : values) 
      {
        int v= Integer.parseInt(value);
        maxValue = Math.max(maxValue, v);
        minValue = Math.min(minValue, v);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max value = ", new String(max));
      context.write(key+":   min value = ", new String(min)); 
  

    }
    

    ///////////////////////////////////////////////////////////////////////////////////
    
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Neighborhood_Analysis_Boundaries")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
      

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "Location")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  

    }
    
    ///////////////////////////////////////////////////////////////////////////////////
     
    maxValue = Integer.MIN_VALUE;
    minValue = Integer.MAX_VALUE;

    if (key== "RowID")
    {
      for (String value : values) 
      {
        String line = value.toString();
        l=line.length();
        maxValue = Math.max(maxValue, l);
        minValue = Math.min(minValue, l);
      }
      String max=Integer.toString(maxValue);
      String min=Integer.toString(minValue);
      context.write(key+":   max length = ", new String(max));
      context.write(key+":   min length = ", new String(min));
  
    }
    
    


   }

}


               


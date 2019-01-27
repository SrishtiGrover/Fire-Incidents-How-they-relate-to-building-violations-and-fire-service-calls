/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */



import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author DELL
 */

public class ProfMapper extends Mapper<LongWritable, Text, Text,IntWritable> {
    /**
     * @param args the command line arguments
     */
    @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        // TODO code application logic here
         String input[]  = value.toString().split("\"");
         
         if(input.length==2)
         {
             
             String rest = input[0];
             String in1[] = rest.split(",");
             
             context.write(new Text("ComplaintNumber"), new IntWritable(in1[0].length()));
             context.write(new Text("ItemSequence"), new IntWritable(in1[1].length()));
             context.write(new Text("DateFiled"), new IntWritable(in1[2].length()));
             context.write(new Text("Block"), new IntWritable(in1[3].length()));
             context.write(new Text("Lot"), new IntWritable(in1[4].length()));
             context.write(new Text("StreetNumber"), new IntWritable(in1[5].length()));
             context.write(new Text("StreetName"), new IntWritable(in1[6].length()));
             context.write(new Text("StreetSuffix"), new IntWritable(in1[7].length()));
             context.write(new Text("Unit"), new IntWritable(in1[8].length()));
             context.write(new Text("Status"), new IntWritable(in1[9].length()));
             context.write(new Text("ReceivingDivision"), new IntWritable(in1[10].length()));
             context.write(new Text("AssignedDivision"), new IntWritable(in1[11].length()));
             context.write(new Text("NOVCategoryDescription"), new IntWritable(in1[12].length()));
             context.write(new Text("Item"), new IntWritable(in1[13].length()));
             context.write(new Text("NOVItemDescription"), new IntWritable(in1[14].length()));
             context.write(new Text("Neighborhoods-AnalysisBoundries"), new IntWritable(in1[15].length()));
             context.write(new Text("SupervisorDistrict"), new IntWritable(in1[16].length()));
             context.write(new Text("Zipcode"), new IntWritable(in1[17].length()));
             
             context.write(new Text("Location"), new IntWritable(input[1].length()));
           
            // Complaint Number,Item Sequence Number,Date Filed,Block,Lot,Street Number,Street Name,Street Suffix,Unit,Status,Receiving Division,Assigned Division,NOV Category Description,Item,NOV Item Description,Neighborhoods - Analysis Boundaries,Supervisor District,Zipcode,Location

         }
         
         else if(input.length==6)
         {
             String rest1 = input[0];
             
             String in1[] = rest1.split(",");
             
             context.write(new Text("ComplaintNumber"), new IntWritable(in1[0].length()));
             context.write(new Text("ItemSequence"), new IntWritable(in1[1].length()));
             context.write(new Text("DateFiled"), new IntWritable(in1[2].length()));
             context.write(new Text("Block"), new IntWritable(in1[3].length()));
             context.write(new Text("Lot"), new IntWritable(in1[4].length()));
             context.write(new Text("StreetNumber"), new IntWritable(in1[5].length()));
             context.write(new Text("StreetName"), new IntWritable(in1[6].length()));
             context.write(new Text("StreetSuffix"), new IntWritable(in1[7].length()));
             context.write(new Text("Unit"), new IntWritable(in1[8].length()));
             context.write(new Text("Status"), new IntWritable(in1[9].length()));
             context.write(new Text("ReceivingDivision"), new IntWritable(in1[10].length()));
             context.write(new Text("AssignedDivision"), new IntWritable(in1[11].length()));
             context.write(new Text("NOVCategoryDescription"), new IntWritable(in1[12].length()));
        
         
             context.write(new Text("Item"), new IntWritable(input[1].length()));
             
              context.write(new Text("NOVItemDescription"), new IntWritable(input[2].length()));
              
              String rest2 = input[4];
              StringTokenizer in2 = new StringTokenizer(rest2,",");
              
              context.write(new Text("SupervisorDist"), new IntWritable(in2.nextToken().length()));
             context.write(new Text("ZipCode"), new IntWritable(in2.nextToken().length()));
             
             context.write(new Text("Location"), new IntWritable(input[5].length()));  
             
         }
         
         else if(input.length==4){
             String rest1 = input[0];
             String in1 []= rest1.split(",");
             if(in1.length==13)
             {
             context.write(new Text("ComplaintNumber"), new IntWritable(in1[0].length()));
             context.write(new Text("ItemSequence"), new IntWritable(in1[1].length()));
             context.write(new Text("DateFiled"), new IntWritable(in1[2].length()));
             context.write(new Text("Block"), new IntWritable(in1[3].length()));
             context.write(new Text("Lot"), new IntWritable(in1[4].length()));
             context.write(new Text("StreetNumber"), new IntWritable(in1[5].length()));
             context.write(new Text("StreetName"), new IntWritable(in1[6].length()));
             context.write(new Text("StreetSuffix"), new IntWritable(in1[7].length()));
           context.write(new Text("Unit"), new IntWritable(in1[8].length()));
             context.write(new Text("Status"), new IntWritable(in1[9].length()));
             context.write(new Text("ReceivingDivision"), new IntWritable(in1[10].length()));
             context.write(new Text("AssignedDivision"), new IntWritable(in1[11].length()));
             context.write(new Text("NOVCategoryDescription"), new IntWritable(in1[12].length()));
             
             context.write(new Text("item"), new IntWritable(input[1].length()));
             
              String rest2 = input[2];
              String in2[] = rest2.split(",");
              
               context.write(new Text("NOVItemDescription"), new IntWritable(in2[0].length()));
              context.write(new Text("SupervisorDist"), new IntWritable(in2[1].length()));
             context.write(new Text("ZipCode"), new IntWritable(in2[2].length()));
             
             context.write(new Text("Location"), new IntWritable(input[3].length()));  
             
                 
             }
             else if(in1.length==14)
             {
                 context.write(new Text("ComplaintNumber"), new IntWritable(in1[0].length()));
             context.write(new Text("ItemSequence"), new IntWritable(in1[1].length()));
             context.write(new Text("DateFiled"), new IntWritable(in1[2].length()));
             context.write(new Text("Block"), new IntWritable(in1[3].length()));
             context.write(new Text("Lot"), new IntWritable(in1[4].length()));
             context.write(new Text("StreetNumber"), new IntWritable(in1[5].length()));
             context.write(new Text("StreetName"), new IntWritable(in1[6].length()));
             context.write(new Text("StreetSuffix"), new IntWritable(in1[7].length()));
             context.write(new Text("Unit"), new IntWritable(in1[8].length()));
             context.write(new Text("Status"), new IntWritable(in1[9].length()));
             context.write(new Text("ReceivingDivision"), new IntWritable(in1[10].length()));
             context.write(new Text("AssignedDivision"), new IntWritable(in1[11].length()));
             context.write(new Text("NOVCategoryDescription"), new IntWritable(in1[12].length()));
              context.write(new Text("Item"), new IntWritable(in1[13].length()));
              
              context.write(new Text("NOVItemDescription"), new IntWritable(input[2].length()));
              
              String rest2 = input[3];
              StringTokenizer in2 = new StringTokenizer(rest2,",");
              
               context.write(new Text("SupervisorDist"), new IntWritable(in2.nextToken().length()));
             context.write(new Text("ZipCode"), new IntWritable(in2.nextToken().length()));
             
              context.write(new Text("Location"), new IntWritable(input[4].length())); 
                 
             }
         }
        /*
         ComplaintNumber string, ItemSequence int , DateFiled date, Block int, Lot int, StreetNumber int,
         StreetName string , StreetSuffix char, Unit char, Status string, ReceivingDivision string, AssignedDivision string,
         NOVCategoryDescription string, Item string , NovItemDescription string, Neighborhoods-AnalysisBoundaries string, SupervisorDistrict int, Zipcode int , Location string
         */
}
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author DELL
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CleanMRJob {

    public static void main(String[] args) throws Exception
        {
                if (args.length != 2)
                {
                        System.err.println("Usage: Page Rank  <input path> <output path>");
                        System.exit(-1);
                }

                Job job = new Job();
                job.setJarByClass(CleanMRJob.class);
                job.setJobName("Cleaning");
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setMapperClass(CleanMapper.class);
                job.setReducerClass(CleanReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
               // job.setNumReduceTasks(1);

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}


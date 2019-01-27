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

public class ProfMRJob {

    public static void main(String[] args) throws Exception
        {
                if (args.length != 2)
                {
                        System.err.println("Usage: Page Rank  <input path> <output path>");
                        System.exit(-1);
                }

                Job job = new Job();
                job.setJarByClass(ProfMRJob.class);
                job.setJobName("Profiling");
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.setMapperClass(ProfMapper.class);
                job.setReducerClass(ProfReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
               // job.setNumReduceTasks(1);

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}

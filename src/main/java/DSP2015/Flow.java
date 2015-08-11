package DSP2015;


import DSP2015.aggregation.Aggregation;
import DSP2015.aggregation.AggregationComparator;
import DSP2015.aggregation.AggregationGroupingComparator;
import DSP2015.aggregation.AggregationPartitioner;
import DSP2015.decrypt.Decrypt;
import DSP2015.totalCount.TotalCount;
import DSP2015.totalCount.TotalCountComparator;
import DSP2015.totalCount.TotalCountGroupingComparator;
import DSP2015.totalCount.TotalCountPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by barashe on 8/10/15.
 */
public class Flow extends Configured implements Tool  {




    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        //conf.set("mapred.job.tracker", "local");
        //conf.set("fs.default.name", "file:////");
        //conf.set("mapred.map.tasks","10");
        //conf.set("mapred.reduce.tasks","10");
        /*conf.setBoolean("stop", (Integer.parseInt(args[5]) == 1 ? true : false));
        conf.set("language", args[4]);*/
        //final String inter = "/home/barashe/Documents/DSP3/inter";
        //final String inter2 = "/home/barashe/Documents/DSP3/inter2";
        final String inter = "/inter";
        final String inter2 = "/inter2";
        final String inter3 = "/inter3";

        Job job1 = Job.getInstance(conf, "Aggregation");
        job1.setJarByClass(Flow.class);
        job1.setMapperClass(Aggregation.MapClass.class);
        job1.setCombinerClass(Aggregation.AggregationCombiner.class);
        job1.setCombinerKeyGroupingComparatorClass(AggregationGroupingComparator.class);
        job1.setReducerClass(Aggregation.ReduceClass.class);
        job1.setPartitionerClass(AggregationPartitioner.class);
        job1.setSortComparatorClass(AggregationComparator.class);
        job1.setGroupingComparatorClass(AggregationGroupingComparator.class);
        job1.setMapOutputKeyClass(PathKey.class);
        job1.setMapOutputValueClass(PathValue.class);

        job1.setOutputKeyClass(PathKey.class);
        job1.setOutputValueClass(PathValue.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(inter));
        //FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        System.out.println("JOB 1 completed");

        Configuration conf2 = new Configuration();
        //conf2.set("mapreduce.job.maps","10");
        //conf2.set("mapreduce.job.reduces","10");

        Job job2 = Job.getInstance(conf2, "Total Count");
        job2.setJarByClass(TotalCount.class);
        job2.setMapperClass(TotalCount.MapClass.class);
        job2.setReducerClass(TotalCount.ReduceClass.class);

        job2.setPartitionerClass(TotalCountPartitioner.class);
        job2.setSortComparatorClass(TotalCountComparator.class);
        job2.setGroupingComparatorClass(TotalCountGroupingComparator.class);
        job2.setCombinerClass(TotalCount.CombinerClass.class);
        job2.setCombinerKeyGroupingComparatorClass(TotalCountGroupingComparator.class);
        job2.setMapOutputKeyClass(PathKey.class);
        job2.setMapOutputValueClass(PathValue.class);
        // Set the outputs
        job2.setOutputKeyClass(PathKey.class);
        job2.setOutputValueClass(PathValue.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(inter));
        FileOutputFormat.setOutputPath(job2, new Path(inter2));
        //FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        job2.waitForCompletion(true);

        System.out.println("JOB 2 completed");

        Configuration conf3 = new Configuration();
        //conf3.set("mapreduce.job.maps","10");
        //conf3.set("mapreduce.job.reduces","10");

        Job job3 = Job.getInstance(conf3, "Decrypt");
        job3.setJarByClass(Decrypt.class);
        job3.setMapperClass(Decrypt.MapClass.class);
        job3.setReducerClass(Decrypt.ReduceClass.class);

        job3.setPartitionerClass(AggregationPartitioner.class);
        job3.setSortComparatorClass(AggregationComparator.class);
        job3.setGroupingComparatorClass(AggregationGroupingComparator.class);
        job3.setMapOutputKeyClass(PathKey.class);
        job3.setMapOutputValueClass(PathValue.class);
        // Set the outputs
        //job3.setOutputKeyClass(Ngram.class);
        //job3.setOutputValueClass(NgramValue.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        //job3.setOutputFormatClass(FileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(inter2));
        //FileOutputFormat.setOutputPath(job3, new Path(inter3));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);

        System.out.println("JOB 3 completed");
/*
        Configuration conf3 = new Configuration();
        //conf3.set("mapreduce.job.maps","10");
        //conf3.set("mapreduce.job.reduces","10");

        Job job3 = Job.getInstance(conf3, "Pmi");
        job3.setJarByClass(Ncount.class);
        job3.setMapperClass(Job3.Pmi.MapClass.class);
        job3.setReducerClass(Job3.Pmi.ReduceClass.class);

        job3.setPartitionerClass(NgramPartitioner.class);
        job3.setSortComparatorClass(Job3.SecondWordComparator.class);
        job3.setGroupingComparatorClass(Job3.SecondWordGroupingComparator.class);
        job3.setMapOutputKeyClass(Ngram.class);
        job3.setMapOutputValueClass(NgramValue.class);
        // Set the outputs
        job3.setOutputKeyClass(Ngram.class);
        job3.setOutputValueClass(NgramValue.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(inter2));
        FileOutputFormat.setOutputPath(job3, new Path(inter3));

        job3.waitForCompletion(true);

        System.out.println("JOB 3 completed");

        Configuration conf4 = new Configuration();
        //conf4.set("mapreduce.job.maps","10");
        //conf4.set("mapreduce.job.reduces","10");

        conf4.set("minPmi", args[2]);
        conf4.set("relMinPmi", args[3]);

        Job job4 = Job.getInstance(conf4, "Pmi");
        job4.setJarByClass(Ncount.class);
        job4.setMapperClass(Job4.RelativePmi.MapClass.class);
        job4.setReducerClass(Job4.RelativePmi.ReduceClass.class);

        job4.setPartitionerClass(NgramPartitioner.class);
        job4.setSortComparatorClass(Job4.RelativePmiComparator.class);
        job4.setGroupingComparatorClass(Job4.RelativePmiGroupingComparator.class);
        job4.setMapOutputKeyClass(Ngram.class);
        job4.setMapOutputValueClass(NgramValue.class);
        // Set the outputs
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(inter3));
        FileOutputFormat.setOutputPath(job4, new Path(args[1]));

        job4.waitForCompletion(true);

        System.out.println("JOB 4 completed");
*/
        return 1;
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new Flow(), args);
    }



}
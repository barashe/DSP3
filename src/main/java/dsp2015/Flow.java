package dsp2015;


import dsp2015.aggregation.Aggregation;
import dsp2015.aggregation.AggregationComparator;
import dsp2015.aggregation.AggregationGroupingComparator;
import dsp2015.aggregation.AggregationPartitioner;
import dsp2015.decrypt.Decrypt;
import dsp2015.decrypt.DecryptPartitioner;
import dsp2015.path_slot_count.PathCount;
import dsp2015.path_slot_count.PathCountComparator;
import dsp2015.path_slot_count.PathCountGroupingComparator;
import dsp2015.path_slot_count.PathCountPartitioner;
import dsp2015.similarity.Similarity;
import dsp2015.similarity.SimilarityComparator;
import dsp2015.similarity.SimilarityPartitioner;
import dsp2015.total_count.TotalCount;
import dsp2015.total_count.TotalCountComparator;
import dsp2015.total_count.TotalCountGroupingComparator;
import dsp2015.total_count.TotalCountPartitioner;
import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import dsp2015.word_slot_count.WordCount;
import dsp2015.word_slot_count.WordCountComparator;
import dsp2015.word_slot_count.WordCountGroupingComparator;
import dsp2015.word_slot_count.WordCountPartitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

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

        ///*
        final String inter = "s3n://ranerandsp6/inter";
        final String inter2 = "s3n://ranerandsp6/inter2";
        final String inter3 = "s3n://ranerandsp6/inter3";
        final String inter4 = "s3n://ranerandsp6/inter4";
        //*/

//        final String inter = "/home/ran/Documents/DSP3/inter";
//        final String inter2 = "/home/ran/Documents/DSP3/inter2";
//        final String inter3 = "/home/ran/Documents/DSP3/inter3";
//        final String inter4 = "/home/ran/Documents/DSP3/inter4";
        /*
        final String inter = "/home/barashe/Documents/DSP3/inter";
        final String inter2 = "/home/barashe/Documents/DSP3/inter2";
        final String inter3 = "/home/barashe/Documents/DSP3/inter3";
        final String inter4 = "/home/barashe/Documents/DSP3/inter4";
        */
        /*
        final String inter = args[1] + "/inter";
        final String inter2 = args[1] + "/inter2";
        final String inter3 = args[1] + "/inter3";
        final String inter4 = args[1] + "/inter4";
        */
        String commaSepartedPaths = createPaths(Integer.valueOf(args[0]));

        conf.set("dpMinCount", args[2]);
        //conf.set("fs.s3n.impl","org.apache.hadoop.fs.s3native.NativeS3FileSystem");
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
        job1.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPaths(job1, commaSepartedPaths);
        FileOutputFormat.setOutputPath(job1, new Path(inter));
        //FileInputFormat.addInputPath(job1, new Path(args[0]));

        job1.waitForCompletion(true);

        System.out.println("JOB 1 completed");

        Configuration conf2 = new Configuration();
        //conf2.set("mapreduce.job.maps","10");
        //conf2.set("mapreduce.job.reduces","10");
        conf2.set("minFeatNum", args[3]);
        Job job2 = Job.getInstance(conf2, "Path Count");
        job2.setJarByClass(PathCount.class);
        job2.setMapperClass(TotalCount.MapClass.class);
        job2.setReducerClass(PathCount.ReduceClass.class);

        job2.setPartitionerClass(PathCountPartitioner.class);
        job2.setSortComparatorClass(PathCountComparator.class);
        job2.setGroupingComparatorClass(PathCountGroupingComparator.class);
        job2.setCombinerClass(TotalCount.CombinerClass.class);
        job2.setCombinerKeyGroupingComparatorClass(PathCountGroupingComparator.class);
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

        Job job3 = Job.getInstance(conf3, "Word Count");
        job3.setJarByClass(WordCount.class);
        job3.setMapperClass(TotalCount.MapClass.class);
        job3.setReducerClass(WordCount.ReduceClass.class);

        job3.setPartitionerClass(WordCountPartitioner.class);
        job3.setSortComparatorClass(WordCountComparator.class);
        job3.setGroupingComparatorClass(WordCountGroupingComparator.class);
        job3.setCombinerClass(TotalCount.CombinerClass.class);
        job3.setCombinerKeyGroupingComparatorClass(WordCountGroupingComparator.class);
        job3.setMapOutputKeyClass(PathKey.class);
        job3.setMapOutputValueClass(PathValue.class);
        // Set the outputs
        job3.setOutputKeyClass(PathKey.class);
        job3.setOutputValueClass(PathValue.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job3, new Path(inter2));
        FileOutputFormat.setOutputPath(job3, new Path(inter3));
        //FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.waitForCompletion(true);

        System.out.println("JOB 3 completed");

        Configuration conf4 = new Configuration();
        //conf4.set("mapreduce.job.maps","10");
        //conf4.set("mapreduce.job.reduces","2");


        Job job4 = Job.getInstance(conf4, "Total Count");
        job4.setJarByClass(TotalCount.class);
        job4.setMapperClass(TotalCount.MapClass.class);
        job4.setReducerClass(TotalCount.ReduceClass.class);

        job4.setPartitionerClass(TotalCountPartitioner.class);
        job4.setSortComparatorClass(TotalCountComparator.class);
        job4.setGroupingComparatorClass(TotalCountGroupingComparator.class);
        job4.setCombinerClass(TotalCount.CombinerClass.class);
        job4.setCombinerKeyGroupingComparatorClass(TotalCountGroupingComparator.class);
        job4.setMapOutputKeyClass(PathKey.class);
        job4.setMapOutputValueClass(PathValue.class);
        // Set the outputs
        //job4.setOutputKeyClass(PathKey.class);
        //job4.setOutputValueClass(PathFeatValue.class);
        job4.setInputFormatClass(SequenceFileInputFormat.class);
        //job4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(inter3));
        FileOutputFormat.setOutputPath(job4, new Path(inter4));
        //FileOutputFormat.setOutputPath(job4, new Path(args[1]));
        job4.waitForCompletion(true);

        System.out.println("JOB 4 completed");
        /*
        Configuration conf5 = new Configuration();
        //conf5.set("mapreduce.job.maps","10");
        //conf5.set("mapreduce.job.reduces","10");

        Job job5 = Job.getInstance(conf5, "Decrypt");
        job5.setJarByClass(Decrypt.class);
        job5.setMapperClass(Decrypt.MapClass.class);
        job5.setReducerClass(Decrypt.ReduceClass.class);

        job5.setPartitionerClass(DecryptPartitioner.class);
        job5.setSortComparatorClass(AggregationComparator.class);
        job5.setGroupingComparatorClass(AggregationGroupingComparator.class);
        job5.setMapOutputKeyClass(PathKey.class);
        job5.setMapOutputValueClass(PathFeatValue.class);
        // Set the outputs
        //job5.setOutputKeyClass(Ngram.class);
        job5.setOutputValueClass(DoubleWritable.class);
        job5.setInputFormatClass(SequenceFileInputFormat.class);
        //job5.setOutputFormatClass(FileOutputFormat.class);
        FileInputFormat.addInputPath(job5, new Path(inter4));
        //FileOutputFormat.setOutputPath(job5, new Path(inter3));
        FileOutputFormat.setOutputPath(job5, new Path(args[1]+"/features"));
        job5.waitForCompletion(true);

        System.out.println("JOB 5 completed");
        */
        Configuration conf6 = new Configuration();
        //conf6.set("mapreduce.job.maps","10");
        //conf6.set("mapreduce.job.reduces","10");
        //conf6.set("positiveTestSet",positiveTestSet);
        //conf6.set("negativeTestSet",negativeTestSet);

        Job job6 = Job.getInstance(conf6, "Similarity");
        job6.setJarByClass(Similarity.class);
        job6.setMapperClass(Similarity.MapClass.class);
        job6.setReducerClass(Similarity.ReduceClass.class);

//        job6.addCacheFile(new URI(positiveTestSet));
//        job6.addCacheFile(new URI(negativeTestSet));

        job6.setPartitionerClass(SimilarityPartitioner.class);
        job6.setSortComparatorClass(SimilarityComparator.class);
        job6.setGroupingComparatorClass(SimilarityComparator.class);
        job6.setMapOutputKeyClass(PathKey.class);
        job6.setMapOutputValueClass(PathFeatValue.class);
        // Set the outputs
        //job6.setOutputKeyClass(Ngram.class);
        //job6.setOutputValueClass(PathFeatValue.class);
        job6.setInputFormatClass(TextInputFormat.class);
        //job6.setOutputFormatClass(FileOutputFormat.class);
        FileInputFormat.addInputPath(job6, new Path(inter4));
        //FileOutputFormat.setOutputPath(job6, new Path(inter3));
        FileOutputFormat.setOutputPath(job6, new Path(args[1]));
        job6.waitForCompletion(true);

        System.out.println("JOB 6 completed");
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

    private String createPaths(int percentage) {
        String template1 = "s3n://dsp152/syntactic-ngram/biarcs/biarcs.";
        String template2 = "-of-99";
        String ret = "";
        for (int i = 0; i < percentage; i++) {
            if (i<10){
                ret = ret + template1 + "0" + i + template2 + ",";
            }
            else{
                ret = ret + template1 + i + template2 + ",";
            }
        }
        return ret.substring( 0 , ret.length()-1);
    }

    public static void main(String[] args) throws Exception {

        ToolRunner.run(new Configuration(), new Flow(), args);
    }



}
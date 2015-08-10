package DSP2015.Step1;

import DSP2015.PathKey;
import DSP2015.PathValue;
import DSP2015.TripletParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/10/15.
 */
public class Ncount {

    public static class MapClass extends Mapper<LongWritable, Text, PathKey, PathValue> {

        private TripletParser p;
        private PathKey pKey = new PathKey();
        private PathValue pValue = new PathValue();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            p = new TripletParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            p.parse(value.toString());
            if(p.getPath()!=null){
                pKey.set(p.getPath().get(0), p.getW1(), true, true);
                pValue.set(p.getPath().get(0), p.getCount());
                pValue.setFirst(true);
                context.write(pKey, pValue);
                pKey.setFirst(false);
                pValue.setFirst(false);
                context.write(pKey, pValue);
                pKey.set(p.getPath().get(0), p.getW2(), false, true);
                pValue.set(p.getPath().get(0), p.getCount());
                pValue.setFirst(true);
                context.write(pKey, pValue);
                pKey.setFirst(false);
                pValue.setFirst(false);
                context.write(pKey, pValue);
            }
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathValue,Text,IntWritable> {

        private Text toSend = new Text();
        private IntWritable intToSend = new IntWritable();

        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PathValue value : values){
                if(value.getIsFirst().get()) {
                    count+= value.getCount().get();
                }
                else{
                    intToSend.set(count);
                    context.write(value.getPath(), intToSend);
                }
            }
        }
    }
}

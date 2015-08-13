package dsp2015.aggregation;

import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import dsp2015.TripletParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/10/15.
 */
public class Aggregation  {

    public static class MapClass extends Mapper<LongWritable, Text, PathKey, PathValue> {

        private TripletParser p = new TripletParser();
        private PathKey pKey = new PathKey();
        private PathValue pValue = new PathValue();
        private String[] path;
        private int dpMinCount;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            dpMinCount = context.getConfiguration().getInt("dpMinCount", 0);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            p.parse(value.toString());
            if(p.getPath()!=null && p.getCount() >= dpMinCount){
                path = p.getPath();
                for(int i = 0; i < path.length; i++) {
                    pKey.set(path[i], p.getW1(), true, true);
                    pValue.set(path[i], p.getCount());
                    pValue.setFirst(true);
                    context.write(pKey, pValue);
                    pKey.setFirst(false);
                    pValue.setFirst(false);
                    context.write(pKey, pValue);
                    pKey.set(path[i], p.getW2(), false, true);
                    pValue.set(path[i], p.getCount());
                    pValue.setFirst(true);
                    context.write(pKey, pValue);
                    pKey.setFirst(false);
                    pValue.setFirst(false);
                    context.write(pKey, pValue);
                }
            }
        }
    }
    public static class ReduceClass extends Reducer<PathKey,PathValue,PathKey,PathValue> {
        PathValue pValue = new PathValue();

        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            String slot = (key.getSlot().get() ? ". X:" : ". Y:");
            for(PathValue value : values) {
                if (value.getIsFirst().get()) {
                    count += value.getCount().get();
                }
            }
            pValue.set(key.getPath().toString(), count);

            context.write(key, pValue);
        }
    }

    public static class AggregationCombiner extends Reducer<PathKey,PathValue,PathKey,PathValue> {

        PathValue pValue = new PathValue();

        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PathValue value : values) {
                if (value.getIsFirst().get()) {
                    count += value.getCount().get();
                }
            }
            pValue.set(key.getPath().toString(), count);
            pValue.setFirst(true);
            key.setFirst(true);
            context.write(key, pValue);
            pValue.setFirst(false);
            key.setFirst(false);
            context.write(key, pValue);

        }
    }
}

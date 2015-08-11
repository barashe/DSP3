package DSP2015.decrypt;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/11/15.
 */
public class Decrypt {

    public static class MapClass extends Mapper<PathKey, PathValue, PathKey, PathValue> {
        @Override
        protected void map(PathKey key, PathValue value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathValue,Text,IntWritable> {

        private Text textToSent = new Text();
        private IntWritable intToSend = new IntWritable();

        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            String line = "";
            int count, nCount;
            for(PathValue value:values){
                line += key.getPath();
                String xy = (key.getSlot().get()? "X":"Y");
                line += ", " + xy + ": " + key.getWord() + ", Count: " + value.getCount().get() + ", Total count: ";
                textToSent.set(line);
                intToSend.set(value.getTotalCount().get());
                context.write(textToSent, intToSend);
            }
        }
    }
}

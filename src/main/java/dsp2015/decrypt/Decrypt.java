package dsp2015.decrypt;

import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/11/15.
 */
public class Decrypt {

    public static class MapClass extends Mapper<PathKey, PathFeatValue, PathKey, PathFeatValue> {
        @Override
        protected void map(PathKey key, PathFeatValue value, Context context) throws IOException, InterruptedException {
            value.setWord(key.getWord());
            context.write(key, value);
        }
    }



    public static class ReduceClass extends Reducer<PathKey,PathFeatValue,Text,DoubleWritable> {

        private Text textToSent = new Text();
        private DoubleWritable intToSend = new DoubleWritable();
        @Override
        protected void reduce(PathKey key, Iterable<PathFeatValue> values, Context context) throws IOException, InterruptedException {
            String line = "";
            for(PathFeatValue value:values){
                line += key.getPath();
                String xy = (key.getSlot().get()? "X":"Y");
                line += ", " + xy + ": " + key.getWord() + ", Count: " + value.getCount().get()+ ", mi: " +value.getMi().get()+ ", tfidf: " +value.getTfidf().get()+ ", Dice: ";
                textToSent.set(line);
                intToSend.set(value.getDice().get());
                context.write(textToSent, intToSend);
            }
        }
    }
}

package dsp2015.word_slot_count;

import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/12/15.
 */
public class WordCount {


    public static class ReduceClass extends Reducer<PathKey,PathValue,PathKey,PathValue> {
        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PathValue value : values){
                if(value.getIsFirst().get()) {
                    count+= value.getCount().get();
                }
                else{

                    value.setWordSlotCount(count);
                    context.write(key, value);
                }
            }
        }
    }
}

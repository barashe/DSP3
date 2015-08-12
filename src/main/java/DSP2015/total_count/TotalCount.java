package DSP2015.total_count;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by barashe on 8/11/15.
 */
public class TotalCount {

    public static class MapClass extends Mapper<PathKey, PathValue, PathKey, PathValue> {
        @Override
        protected void map(PathKey key, PathValue value, Context context) throws IOException, InterruptedException {
            key.setFirst(true);
            value.setFirst(true);
            context.write(key, value);
            key.setFirst(false);
            value.setFirst(false);
            context.write(key, value);
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathValue,PathKey,PathValue> {
        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(PathValue value : values){
                if(value.getIsFirst().get()) {
                    count+= value.getCount().get();
                }
                else{
                    value.setTotalCount(count);
                    context.write(key, value);
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<PathKey,PathValue,PathKey,PathValue> {
        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            boolean sendFirst = true;
            for(PathValue value : values){
                if(value.getIsFirst().get()) {
                    count+= value.getCount().get();
                }
                else{
                    if(sendFirst){
                        PathKey tmpKey = new PathKey();
                        tmpKey.set(key);
                        PathValue tmpValue = new PathValue(value);
                        tmpKey.setFirst(true);
                        tmpValue.setFirst(true);
                        tmpValue.setCount(count);
                        context.write(tmpKey, tmpValue);
                        sendFirst = false;
                    }

                    context.write(key, value);
                }
            }
        }
    }

}

package dsp2015.total_count;

import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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
            value.setWord(key.getWord());
            context.write(key, value);
            key.setFirst(false);
            value.setFirst(false);
            context.write(key, value);
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathValue,Text,DoubleWritable> {


        private StatComp stat = new StatComp();
        private Text toSend = new Text();
        private DoubleWritable doubleToSend = new DoubleWritable();


        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            String tmp, slot = (key.getSlot().get()? "X" : "Y");
            for(PathValue value : values){
                if(value.getIsFirst().get()) {
                    count+= value.getCount().get();
                }

                else{
                    value.setTotalCount(count);
                    stat.comp(value);
                    //tmp = key.getPath() +"\t" +slot + "\t" + value.getWord()+ "\t" + stat.getMi()+ "\t" + stat.getTfidf();
                    tmp = key.getPath() +"\t" +slot + "\t" + value.getWord()+ "\t" + stat.getMi()+ "\t" + stat.getTfidf()  + "\t" + stat.getDice()  + "\t" + value.getCount() + "\t" + value.getTotalCount() + "\t" + value.getWordSlotCount();
                    //doubleToSend.set(stat.getDice());
                    doubleToSend.set(value.getTotalPathSlotCount().get());
                    toSend.set(tmp);
                    context.write(toSend, doubleToSend);
                    /*value.setTotalCount(count);
                    toSend.set(value);
                    stat.comp(value);
                    toSend.setStat(stat.getMi(), stat.getTfidf(), stat.getDice());
                    context.write(key, toSend);
                    */
                }
            }
        }
    }

    public static class CombinerClass extends Reducer<PathKey,PathValue,PathKey,PathValue> {
        @Override
        protected void reduce(PathKey key, Iterable<PathValue> values, Context context) throws IOException, InterruptedException {
            long count = 0;
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

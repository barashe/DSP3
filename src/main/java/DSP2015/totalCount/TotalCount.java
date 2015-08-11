package DSP2015.totalCount;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by barashe on 8/11/15.
 */
public class TotalCount {

    public static class MapClass extends Mapper<PathKey, PathValue, PathKey, PathValue> {
        @Override
        protected void map(PathKey key, PathValue value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }




}

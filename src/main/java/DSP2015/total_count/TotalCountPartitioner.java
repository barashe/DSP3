package DSP2015.total_count;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/11/15.
 */
public class TotalCountPartitioner extends Partitioner<PathKey, PathValue> {
    @Override
    public int getPartition(PathKey pathKey, PathValue pathValue, int i) {
        int n = (pathKey.getSlot().get()? 1:0);
        return n % i;
    }
}
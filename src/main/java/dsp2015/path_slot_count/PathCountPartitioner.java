package dsp2015.path_slot_count;

import dsp2015.PathKey;
import dsp2015.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/12/15.
 */
public class PathCountPartitioner  extends Partitioner<PathKey, PathValue> {
    @Override
    public int getPartition(PathKey pathKey, PathValue pathValue, int i) {
        int n = pathKey.getPath().hashCode() + 163* (pathKey.getSlot().get()?11:7);
        return n % i;
    }
}

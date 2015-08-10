package DSP2015.Step1;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/10/15.
 */
public class Step1Partitioner extends Partitioner<PathKey, PathValue> {
    @Override
    public int getPartition(PathKey pathKey, PathValue pathValue, int i) {
        int n = (pathKey.getSlot().get()? 1:0);
        return n % i;
    }
}

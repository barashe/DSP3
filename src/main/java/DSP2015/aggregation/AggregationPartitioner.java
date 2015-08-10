package DSP2015.aggregation;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by barashe on 8/10/15.
 */
public class AggregationPartitioner extends Partitioner<PathKey, PathValue> {

    @Override
    public int getPartition(PathKey pathKey, PathValue pathValue, int i) {
        return pathKey.getPath().hashCode() % i;
    }
}

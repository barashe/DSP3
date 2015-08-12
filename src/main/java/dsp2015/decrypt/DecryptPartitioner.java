package dsp2015.decrypt;

import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;

import dsp2015.types.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/12/15.
 */
public class DecryptPartitioner extends Partitioner<PathKey, PathFeatValue> {

    @Override
    public int getPartition(PathKey pathKey, PathFeatValue pathValue, int i) {
        return Math.abs(pathKey.getPath().hashCode() % i);
    }

}

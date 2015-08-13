package dsp2015.similarity;

import dsp2015.types.PathKey;
import dsp2015.types.PathFeatValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/13/15.
 */
public class SimilarityPartitioner extends Partitioner<PathKey, PathFeatValue> {
    @Override
    public int getPartition(PathKey pathKey, PathFeatValue pathFeatValue, int i) {
        return Math.abs(pathKey.getSimKey().hashCode() % i);
    }
}

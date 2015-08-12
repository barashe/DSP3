package DSP2015.word_slot_count;

import DSP2015.PathKey;
import DSP2015.PathValue;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by barashe on 8/12/15.
 */
public class WordCountPartitioner extends Partitioner<PathKey, PathValue> {
    @Override
    public int getPartition(PathKey pathKey, PathValue pathValue, int i) {
        int n = pathKey.getWord().hashCode() + 163* (pathKey.getSlot().get()?11:7);
        return n % i;

    }
}

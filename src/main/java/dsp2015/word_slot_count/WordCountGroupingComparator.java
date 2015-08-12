package dsp2015.word_slot_count;

import dsp2015.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/12/15.
 */
public class WordCountGroupingComparator  extends WritableComparator {

    public WordCountGroupingComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        PathKey pk = (PathKey) a;
        PathKey pk2 = (PathKey) b;

        int res = pk2.getSlot().compareTo(pk.getSlot());
        if(res == 0)
            res = pk.getWord().compareTo(pk2.getWord());
        return res;

    }
}

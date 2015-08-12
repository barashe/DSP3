package dsp2015.path_slot_count;

import dsp2015.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/12/15.
 */
public class PathCountComparator extends WritableComparator {
    public PathCountComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PathKey pk = (PathKey) a;
        PathKey pk2 = (PathKey) b;

        int res = pk2.getSlot().compareTo(pk.getSlot());
        if(res == 0) {
            res = pk.getPath().compareTo(pk2.getPath());
            if (res ==0)
                res = pk2.getIsFirst().compareTo(pk.getIsFirst());
        }
        return res;
    }
}

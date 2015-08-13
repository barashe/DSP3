package dsp2015.total_count;

import dsp2015.types.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/11/15.
 */
public class TotalCountComparator extends WritableComparator {
    public TotalCountComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PathKey p1 = (PathKey)a;
        PathKey p2 = (PathKey)b;
        int res = p2.getSlot().compareTo(p1.getSlot());
        if(res ==0){
            res = p2.getIsFirst().compareTo(p1.getIsFirst());
            if(res == 0){
                res = p1.getPath().compareTo(p2.getPath());
            }
        }
        return res;
    }
}

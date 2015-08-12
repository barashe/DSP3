package dsp2015.aggregation;

import dsp2015.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/10/15.
 */
public class AggregationGroupingComparator extends WritableComparator {
    public AggregationGroupingComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PathKey p1 = (PathKey)a;
        PathKey p2 = (PathKey)b;
        int res = p1.getPath().compareTo(p2.getPath());
        if (res ==0){
            res = p1.getSlot().compareTo(p2.getSlot());
            if (res ==0){
                res = p1.getWord().compareTo(p2.getWord());
            }
        }
        return res;
    }
}

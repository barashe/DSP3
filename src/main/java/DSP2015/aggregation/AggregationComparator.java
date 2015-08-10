package DSP2015.aggregation;

import DSP2015.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/10/15.
 */
public class AggregationComparator extends WritableComparator {
    public AggregationComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PathKey p1 = (PathKey)a;
        PathKey p2 = (PathKey)b;
        int res = p1.getPath().compareTo(p2.getPath());
        if (res ==0){
            res = p2.getSlot().compareTo(p1.getSlot());
            if (res ==0){
                res = p1.getWord().compareTo(p2.getWord());
                if(res ==0) {
                    res = p2.getIsFirst().compareTo(p1.getIsFirst());
                }
            }
        }
        return res;
    }
}

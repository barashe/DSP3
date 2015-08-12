package DSP2015.total_count;

import DSP2015.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by barashe on 8/11/15.
 */
public class TotalCountGroupingComparator extends WritableComparator {
    public TotalCountGroupingComparator() {
        super(PathKey.class, true);
    }
    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
        PathKey pk = (PathKey) tp1;
        PathKey pk2 = (PathKey) tp2;
        return pk.getSlot().compareTo(pk2.getSlot());
    }
}

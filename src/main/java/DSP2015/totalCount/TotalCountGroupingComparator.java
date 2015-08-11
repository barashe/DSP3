package DSP2015.totalCount;

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
        PathKey PathKey = (PathKey) tp1;
        PathKey PathKey2 = (PathKey) tp2;
        return PathKey.getSlot().compareTo(PathKey2.getSlot());
    }
}

package dsp2015.similarity;

import dsp2015.types.PathKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ran on 14/08/15.
 */
public class SimilarityComparator extends WritableComparator {
    public SimilarityComparator() {
        super(PathKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        PathKey p1 = (PathKey)a;
        PathKey p2 = (PathKey)b;
        int res = p2.getSimKey().compareTo(p1.getSimKey());
//        if(res ==0){
//            res = p2.getIsFirst().compareTo(p1.getIsFirst());
//            if(res == 0){
//                res = p1.getPath().compareTo(p2.getPath());
//            }
//        }
        return res;
    }
}

package DSP2015;

import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * Created by barashe on 8/9/15.
 */
public class TripletParserTest extends TestCase {

    public void testCount() throws Exception {
        TripletParser p = new TripletParser();
        p.parse("skulk\tskulk/VB/xcomp/0 like/IN/prep/1 whipped/VBN/amod/4 cur/RP/pobj/2\t13\t1863,2\t1923,3\t1924,2\t1925,3\t1933,1\t1946,1\t1949,1");
        Assert.assertEquals(p.getCount(), 13);
    }

    public void testPath() throws Exception{
        TripletParser p = new TripletParser();
        p.parse("skulk\tskulk/VB/xcomp/0 like/IN/prep/1 whipped/VBN/amod/4 cur/RP/pobj/2\t13\t1863,2\t1923,3\t1924,2\t1925,3\t1933,1\t1946,1\t1949,1");
        Assert.assertEquals(p.getPath(), "skulk/VB/xcomp/0 like/IN/prep/1 whipped/VBN/amod/4 cur/RP/pobj/2");
    }

}
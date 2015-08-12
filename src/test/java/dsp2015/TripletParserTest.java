package dsp2015;

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
        p.parse("slapped\tslapped/VBD/ROOT/0 him/PRP/dobj/1 on/IN/prep/1 cheek/NN/pobj/3\t85\t1828,1\t1873,1\t1884,2\t1907,1\t1911,1\t1920,1\t1932,2\t1933,4\t1934,1\t1936,1\t1941,1\t1943,2\t1944,1\t1945,9\t1946,3\t1947,1\t1948,1\t1949,2\t1950,2\t1951,1\t1952,1\t1956,3\t1961,1\t1962,1\t1964,2\t1967,1\t1973,2\t1974,2\t1976,1\t1978,3\t1979,2\t1980,1\t1981,1\t1982,2\t1983,1\t1985,2\t1986,1\t1987,1\t1988,1\t1991,2\t1992,1\t1993,2\t1994,1\t1998,1\t1999,2\t2001,2\t2002,2\t2003,1\t2004,1\t2006,2\t2008,1");
        Assert.assertEquals("slapped X on Y", p.getPath()[0]);
    }

}
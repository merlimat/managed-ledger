package org.apache.bookkeeper.mledger.util;

import org.junit.Assert;
import org.testng.annotations.Test;

public class PairTest {

    /*
     * Has been tested elsewhere
     *
     * @Test public void Pair() { }
     */

    @Test
    public void create() {
        Pair<String, String> p = Pair.create("firstOne", "secondOne");
        Assert.assertEquals("firstOne", p.first);
        Assert.assertEquals("secondOne", p.second);
        Integer int3 = new Integer(3);
        Pair<String, Integer> q = Pair.create("firstOne", int3);
        Assert.assertEquals("firstOne", q.first);
        Assert.assertEquals(int3, q.second);
    }

    @Test
    public void toStringTest() {
        Pair<String, String> p = Pair.create("firstOne", "secondOne");
        Assert.assertEquals("firstOne", p.first);
        Assert.assertEquals("secondOne", p.second);
        Assert.assertEquals("(firstOne,secondOne)", p.toString());
    }
}

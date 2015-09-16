package it.factbook.semantic;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSearchWorker {
    @Test
    public void testCommonMems(){
        int[] mem1 = new int[] {1,2,3,4,5,6};
        int[] mem2 = new int[] {7,8,9,10,11,12};
        assertEquals(0, SemanticSearchWorker.commonMems(mem1, mem2, 4));

        mem1 = new int[] {1,2,3,4,5,6};
        mem2 = new int[] {1,2,0,0,3,4};
        assertEquals(4, SemanticSearchWorker.commonMems(mem1, mem2, 4));

        mem1 = new int[] {1,2,0,0,0,4};
        mem2 = new int[] {1,2,3,4,5,6};
        assertEquals(2, SemanticSearchWorker.commonMems(mem1, mem2, 4));
    }
}

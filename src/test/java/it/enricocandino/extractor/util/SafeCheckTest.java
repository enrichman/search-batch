package it.enricocandino.extractor.util;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by enrico on 13/06/15.
 */
public class SafeCheckTest {

    @Test
    public void testIsSafe() throws Exception {
        String text = "";
        boolean isSafe = SafeCheck.INSTANCE.isSafe(text);
        assertTrue(isSafe);
    }
}
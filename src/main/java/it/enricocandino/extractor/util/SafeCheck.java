package it.enricocandino.extractor.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Enrico Candino
 */
public enum SafeCheck {
    
    INSTANCE;

    private static final int KEY_THRESHOLD = 5;
    private Set<String> keywordSet;

    SafeCheck() {
        keywordSet = new HashSet<String>();

        BufferedReader br = null;
        try {

            br = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/xkeyword.txt")));
            String line = br.readLine();

            while (line != null) {
                keywordSet.add(line);
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (br != null)
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }
    
    public boolean isSafe(String text) {
        int count = 0;

        text = text.toLowerCase();
        for(String k : keywordSet) {
            if(text.contains(k)) {
                count++;
                if(count >= KEY_THRESHOLD)
                    return false;
            }
        }
        return true;
    }
    
}

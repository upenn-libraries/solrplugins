package edu.upenn.library.solrplugins.callno;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import junit.framework.TestCase;

/**
 *
 * @author michael
 */
public class MicroNormalizerTest extends TestCase {
    
    private static final Pattern TAB_SPLIT = Pattern.compile("\t", Pattern.LITERAL);
    private static final Map<String, String> testCases = new HashMap<>();
    
    static {
        String classname = MicroNormalizerTest.class.getSimpleName();
        InputStream in = MicroNormalizerTest.class.getClassLoader().getResourceAsStream(classname.substring(0, classname.length() - 4).concat(".test"));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                String[] entry = TAB_SPLIT.split(line);
                if (entry.length == 2) {
                    testCases.put(entry[1], "null".equals(entry[0]) ? null : entry[0]);
                }
            }
            br.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Test of normalize method, of class LCNormalizer.
     */
    public void testNormalize() {
        MicroNormalizer instance = new MicroNormalizer();
        int success = 0;
        for (Entry<String, String> e : testCases.entrySet()) {
            System.out.print("\ttest "+e.getValue() +" <=? "+e.getKey()+" ... ");
            assertEquals(e.getValue(), instance.normalize(e.getKey()));
            System.out.println("success!");
            success++;
        }
        assertEquals(success, testCases.size());
        System.out.println("\t"+success + " of "+testCases.size()+" test cases successful");
    }
    
}
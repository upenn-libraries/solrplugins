package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class MicroNormalizer implements Normalizer {

    static final int alphaLimit = 7;
    static final int numLimit = 5;
    static final String microPattern = "\\s*micro([a-z]+)\\s+([^0-9]*[a-z][^0-9]*)?";
    static final String numberPattern = "([0-9]{1,"+numLimit+"})?";
    static final String repeatNumberPattern = "(?:[^0-9]"+numberPattern+")?";
    static final String other = "(\\s.*)?$";
    static final Pattern p = Pattern.compile(microPattern + numberPattern + repeatNumberPattern + other);
    static final Padder zeroPads;
    static final Padder spacePads;

    static {
        StringBuilder sb = new StringBuilder(Math.max(numLimit, alphaLimit));
        zeroPads = new Padder(sb, numLimit, '0', false);
        spacePads = new Padder(sb, alphaLimit, ' ', true);
    }
    
    private static final Pattern nonAlpha = Pattern.compile("[^a-z]+");
    
    public String normalize(CharSequence input) {
        Matcher m = p.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder(input.length());
            int[] groupIndexes = new int[6];
            int maxIndex = 0;
            for (int i = 1; i <= 5; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        sb.append(s);
                        break;
                    case 2:
                        if (spacePads.appendVariable(sb, s, groupIndexes, i, nonAlpha, "")) maxIndex = i;
                        break;
                    case 3:
                    case 4:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 5:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 4) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();
        }
    }
    
}

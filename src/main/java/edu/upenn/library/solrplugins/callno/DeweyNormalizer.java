package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class DeweyNormalizer implements Normalizer {

    static final int numDecLimit = 3;
    static final int numCutterLimit = 5;
    static final int alphaCutterLimit = 4;
    static final String leading = "([0-9]{1,"+numDecLimit+"})(?:\\.([0-9]{1,"+numDecLimit+"}))?";
    static final String cutter = "([a-z])([0-9]{1,"+numCutterLimit+"})([a-z])?";
    static final String firstCutter = "(?:[^a-z0-9]*"+cutter+")?";
    static final String repeatCutter = "(?:[^a-z0-9]+"+cutter+")?";
    static final String other = "(?:\\s(.+)?)?$";
    static final Pattern p = Pattern.compile(leading + firstCutter + repeatCutter + other);
    static final int indices = 5;
    static final Padder zeroPadsCutter;
    static final Padder zeroPads;
    static final Padder zeroPadsOrdinal;
    static final Padder spacePadsCutter;

    static {
        StringBuilder sb = new StringBuilder();
        zeroPadsCutter = new Padder(sb, numCutterLimit, '0', true);
        spacePadsCutter = new Padder(sb, alphaCutterLimit, ' ', true);
        zeroPads = new Padder(sb, numDecLimit, '0', true);
        zeroPadsOrdinal = new Padder(sb, numDecLimit, '0', false);
    }
    
    public String normalize(CharSequence input) {
        Matcher m = p.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            int[] groupIndexes = new int[10];
            int maxIndex = 0;
            StringBuilder sb = new StringBuilder(input.length());
            for (int i = 1; i <= 9; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        if (zeroPadsOrdinal.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 2:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 3:
                    case 6:
                        if (spacePadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 5:
                    case 8:
                        if (s == null || s.length() < 1) {
                            sb.append(' ');
                        } else {
                            Padder.appendStrict(sb, s.charAt(0), groupIndexes, i);
                            maxIndex = i;
                        }
                        break;
                    case 4:
                    case 7:
                        if (zeroPadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 9:
                        if (Padder.appendDirect(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                }
            }
            if (maxIndex < 8) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();

        }
    }
    
}

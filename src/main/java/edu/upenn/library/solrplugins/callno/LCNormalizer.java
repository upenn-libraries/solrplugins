package edu.upenn.library.solrplugins.callno;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author michael
 */
public class LCNormalizer implements Normalizer {

    static final int alphaLimit = 3;
    static final int numLimit = 4;
    static final int cutterNumLimit = 10;
    static final String classPattern = "^\\s*([a-z]{1,"+alphaLimit+"})[^a-z0-9]*(?:(?:([1-9][0-9]{0,"+(numLimit -1)+"})(?:\\.([0-9]{1,"+numLimit+"}))?)|[^a-z0-9]|$)";
    static final String cutterPattern = "(?:[^a-z0-9]*([a-z]{1,"+alphaLimit+"})(?:([0-9]{1,"+cutterNumLimit+"})|[^a-z0-9]|$))?";
    static final String other = "(\\s.*)?$";
    static final Pattern lcCallNoPattern = Pattern.compile(classPattern + cutterPattern + cutterPattern + other);
    static final String[] examples = {"E",
        "E 184 .A1 G78",
        "E184.A2 G78 1967",
        "E184.A2 G78 1970",
        "EA",
        "EA 10",
        "EA 10 1970",
        "610.5 L22",
        "Portfolio TS1098.R63 M67 2000",
        "Portfolio",
        "EA10 B7",
        "EA 10.B7.G8",
        "n1a98b6 1999",
        "EA10.5"};
    static final Padder spacePads;
    static final Padder zeroPads;
    static final Padder zeroPadsDec;
    static final Padder zeroPadsCutter;
    
    static final int otherNumberLimit = 5;
    static final Pattern otherNumber = Pattern.compile("(?<=^|[^0-9])[0-9]{1," + otherNumberLimit + "}(?=[^0-9]|$)");
    static final Padder zeroPadsOther;
    

    static {
        StringBuilder sb = new StringBuilder(Math.max(alphaLimit, Math.max(numLimit, cutterNumLimit)));
        spacePads = new Padder(sb, alphaLimit, ' ', true);
        zeroPads = new Padder(sb, numLimit, '0', false);
        zeroPadsDec = new Padder(sb, numLimit, '0', true);
        zeroPadsCutter = new Padder(sb, cutterNumLimit, '0', true);
        zeroPadsOther = new Padder(sb, otherNumberLimit, '0', false);
    }
    
    public static void main(String[] args) {
        LCNormalizer lcn = new LCNormalizer();
        for (int i = 0; i < examples.length; i++) {
            System.out.println(lcn.normalize(examples[i].toLowerCase())+"\t\t"+examples[i].toLowerCase());
        }
    }

    private String normalizeOther(String other) {
        Matcher m = otherNumber.matcher(other);
        if (!m.find()) {
            return other;
        } else {
            StringBuffer sb = new StringBuffer();
            StringBuilder tmp = new StringBuilder(otherNumberLimit);
            do {
                zeroPadsOther.appendPadded(tmp, m.group());
                m.appendReplacement(sb, tmp.toString());
                tmp.setLength(0);
            } while (m.find());
            m.appendTail(sb);
            return sb.toString();
        }
    }
    
    @Override
    public String normalize(CharSequence input) {
        Matcher m = lcCallNoPattern.matcher(input);
        if (!m.matches()) {
            return null;
        } else {
            StringBuilder sb = new StringBuilder(input.length() * 2);
            int[] groupIndexes = new int[9];
            int maxIndex = 0;
            for (int i = 1; i <= 8; i++) {
                String s = m.group(i);
                switch (i) {
                    case 1:
                        if (spacePads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 2:
                        if (zeroPads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 3:
                        if (zeroPadsDec.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 4:
                    case 6:
                        if (spacePads.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 5:
                    case 7:
                        if (zeroPadsCutter.appendPadded(sb, s, groupIndexes, i)) maxIndex = i;
                        break;
                    case 8:
                        if (Padder.appendDirect(sb, s == null ? s : normalizeOther(s), groupIndexes, i)) maxIndex = i;
                }
            }
            if (maxIndex < 7) {
                sb.setLength(groupIndexes[maxIndex]);
            }
            return sb.toString();
        }
    }
    
}

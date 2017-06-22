package edu.upenn.library.solrplugins.callno;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 *
 * @author michael
 */
public class TypePrefixFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final char delim;
    private final Set<String> excludeTypes;
    private final Set<String> includeTypes;
    private final String outputType;
    
    public TypePrefixFilter(TokenStream input, char delim, Set<String> include, Set<String> exclude, String outputType) {
        super(input);
        this.delim = delim;
        includeTypes = include;
        excludeTypes = exclude;
        this.outputType = outputType;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
            return false;
        } else {
            String type = typeAtt.type();
            if ((includeTypes == null || includeTypes.contains(type)) && (excludeTypes == null || !excludeTypes.contains(type))) {
                int newLength = type.length() + (delim == -1 ? 0 : 1) + termAtt.length();
                char[] termBuff = termAtt.buffer();
                if (termBuff.length < newLength) {
                    termBuff = termAtt.resizeBuffer(newLength);
                }
                System.arraycopy(termBuff, 0, termBuff, type.length() + (delim == -1 ? 0 : 1), termAtt.length());
                type.getChars(0, type.length(), termBuff, 0);
                if (delim != -1) {
                    termBuff[type.length()] = delim;
                }
                termAtt.setLength(newLength);
                if (outputType != null) {
                    typeAtt.setType(outputType);
                }
            }
            return true;
        }
    }
    
}

package edu.upenn.library.solrplugins.callno;

import java.io.IOException;
import java.util.Map.Entry;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public final class CallNoFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final Entry<String, Normalizer>[] normalizers;
    private final String defaultType;

    public CallNoFilter(TokenStream input, Entry<String, Normalizer>[] normalizers, String defaultType) {
        super(input);
        this.normalizers = normalizers;
        this.defaultType = defaultType;
    }
    
    @Override
    public final boolean incrementToken() throws IOException {
        if (!input.incrementToken()) {
            return false;
        } else {
            for (Entry<String, Normalizer> e : normalizers) {
                String normalized;
                if ((normalized = e.getValue().normalize(termAtt)) != null) {
                    typeAtt.setType(e.getKey());
                    termAtt.setEmpty();
                    termAtt.append(normalized);
                    return true;
                }
            }
            if (defaultType != null) {
                typeAtt.setType(defaultType);
            }
            return true;
        }
    }
    
}

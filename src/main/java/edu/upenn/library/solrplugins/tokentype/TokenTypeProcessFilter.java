package edu.upenn.library.solrplugins.tokentype;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 *
 * @author michael
 */
public class TokenTypeProcessFilter extends TokenFilter {

    private final String inputTypeRename;
    private TokenStream outputFilter;
    private final boolean preserveOriginalType;
    private final Set<String> includeInput;
    private final Set<String> excludeInput;
    
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    
    private Delegate delegate = Delegate.SELF;

    public TokenTypeProcessFilter(TokenStream input, Set<String> includeInput, Set<String> excludeInput, String inputTypeRename) {
        super(input);
        this.preserveOriginalType = inputTypeRename == null;
        this.inputTypeRename = inputTypeRename;
        this.includeInput = includeInput;
        this.excludeInput = excludeInput;
    }

    public void setDelegate(TokenStream outputFilter) {
        this.outputFilter = outputFilter;
    }
    
    private static enum Delegate { SELF, DELEGATE, DELEGATED }
    
    @Override
    public boolean incrementToken() throws IOException {
        if (delegate == Delegate.DELEGATE) {
            delegate = Delegate.DELEGATED;
            return true;
        } else if (delegate == Delegate.DELEGATED) {
            delegate = Delegate.SELF;
            return true;
        } else if (input.incrementToken()) {
            String type = typeAtt.type();
            if ((includeInput == null || includeInput.contains(type)) && (excludeInput == null || !excludeInput.contains(type))) {
                if (!preserveOriginalType) {
                    typeAtt.setType(inputTypeRename);
                }
                delegate = Delegate.DELEGATE;
                int inc = posIncrAtt.getPositionIncrement();
                if (outputFilter.incrementToken()) {
                    State outputState = captureState();
                    boolean multipleTokens = false;
                    while (outputFilter.incrementToken() && delegate == Delegate.DELEGATED) {
                        // TODO this results in double-processing of input; try another way?
                        multipleTokens = true;
                        outputState = captureState();
                    }
                    restoreState(outputState);
                    posIncrAtt.setPositionIncrement(inc); // Ensure position increment remains the same.
                    return true;
                } else {
                    return false;
                }
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
    private boolean ending = false;
    
    @Override
    public void end() throws IOException {
        if (outputFilter == null || ending) {
            ending = false;
            super.end();
        } else {
            ending = true;
            outputFilter.end();
        }
    }

    private boolean resetting = false;
    
    @Override
    public void reset() throws IOException {
        if (outputFilter == null || resetting) {
            resetting = false;
            delegate = Delegate.SELF;
            super.reset();
        } else {
            resetting = true;
            outputFilter.reset();
        }
    }

    private boolean closing = false;
    
    @Override
    public void close() throws IOException {
        if (outputFilter == null || closing) {
            closing = false;
            super.close();
        } else {
            closing = true;
            outputFilter.close();
        }
    }

}

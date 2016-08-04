package edu.upenn.library.solrplugins.tokentype;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

/**
 *
 * @author michael
 */
public class TokenTypeJoinFilter extends TokenFilter {

    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
    private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    
    private final StringBuilder sb = new StringBuilder(200);
    private final String outputType;
    private final char delim;
    private final Map<String, Integer> componentIndexMap;

    private final String[] components;
    private int bufferedOffsetStart;
    private int bufferedOffsetEnd;
    private State state;
    private boolean primed = false;
    private int increment = 0;
    
    public TokenTypeJoinFilter(TokenStream input, String[] componentTypes, String outputType, int delimCodepoint) {
        super(input);
        componentIndexMap = new HashMap<>(componentTypes.length * 2);
        for (int i = 0; i < componentTypes.length; i++) {
            componentIndexMap.put(componentTypes[i], i);
        }
        components = new String[componentTypes.length];
        this.outputType = outputType;
        this.delim = Character.toChars(delimCodepoint)[0];
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (state != null) {
            restoreState(state);
            state = null;
            return buffer();
        } else if (input.incrementToken()) {
            int inc;
            if ((inc = posIncrAtt.getPositionIncrement()) > 0) {
                if (primed) {
                    state = captureState();
                    posIncrAtt.setPositionIncrement(increment);
                    increment = inc;
                    outputJoinedTokens();
                    return true;
                } else {
                    increment = inc;
                    return buffer();
                }
            } else {
                return buffer();
            }
        } else if (primed) {
            posIncrAtt.setPositionIncrement(increment);
            outputJoinedTokens();
            return true;
        } else {
            return false;
        }
    }
    
    private boolean buffer() throws IOException {
        Integer index;
        if ((index = componentIndexMap.get(typeAtt.type())) != null) {
            components[index] = termAtt.toString();
            if (primed) {
                int tmp;
                if ((tmp = offsetAtt.startOffset()) < bufferedOffsetStart) {
                    bufferedOffsetStart = tmp;
                }
                if ((tmp = offsetAtt.endOffset()) > bufferedOffsetEnd) {
                    bufferedOffsetEnd = tmp;
                }
            } else {
                bufferedOffsetStart = offsetAtt.startOffset();
                bufferedOffsetEnd = offsetAtt.endOffset();
                primed = true;
            }
            return incrementToken();
        } else {
            posIncrAtt.setPositionIncrement(increment);
            increment = 0;
            return true;
        }
    }

    private void outputJoinedTokens() {
        sb.setLength(0);
        if (components[0] != null) {
            sb.append(components[0]);
        }
        for (int i = 1; i < components.length; i++) {
            sb.append(delim);
            if (components[i] != null) {
                sb.append(components[i]);
            }
        }
        termAtt.setEmpty();
        termAtt.append(sb);
        typeAtt.setType(outputType);
        offsetAtt.setOffset(bufferedOffsetStart, bufferedOffsetEnd);
        Arrays.fill(components, null);
        primed = false;
    }

    @Override
    public void end() throws IOException {
        super.end();
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public void reset() throws IOException {
        primed = false;
        increment = 0;
        state = null;
        Arrays.fill(components, null);
        super.reset();
    }
    
}

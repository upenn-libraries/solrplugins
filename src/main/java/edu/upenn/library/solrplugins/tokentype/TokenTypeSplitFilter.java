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
public class TokenTypeSplitFilter extends TokenFilter {

  private final String inputTypeRename;
  private final String outputType;
  private TokenStream outputFilter;
  private final boolean preserveOriginalType;
  private final Set<String> includeInput;
  private final Set<String> excludeInput;

  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private State state;
  private Delegate delegate = Delegate.SELF;

  public TokenTypeSplitFilter(TokenStream input, Set<String> includeInput, Set<String> excludeInput, String outputType, String inputTypeRename) {
    super(input);
    this.preserveOriginalType = inputTypeRename == null;
    this.outputType = outputType;
    this.inputTypeRename = inputTypeRename;
    this.includeInput = includeInput;
    this.excludeInput = excludeInput;
  }

  public void setDelegate(TokenStream outputFilter) {
    this.outputFilter = outputFilter;
  }

  private static enum Delegate {

    SELF, DELEGATE, DELEGATED
  };

  @Override
  public boolean incrementToken() throws IOException {
    if (delegate == Delegate.DELEGATE) {
      delegate = Delegate.DELEGATED;
      return true;
    } else if (delegate == Delegate.DELEGATED) {
      delegate = Delegate.SELF;
      return true;
    } else if (state != null) {
      restoreState(state);
      state = null;
      typeAtt.setType(outputType);
      posIncrAtt.setPositionIncrement(0);
      if (outputFilter == null) {
        return true;
      } else {
        delegate = Delegate.DELEGATE;
        if (outputFilter.incrementToken()) {
          State outputState = captureState();
          boolean multipleTokens = false;
          while (outputFilter.incrementToken() && delegate == Delegate.DELEGATED) {
            multipleTokens = true;
            outputState = captureState();
          }
          restoreState(outputState);
          posIncrAtt.setPositionIncrement(0); // reset
          return true;
        } else {
          return false;
        }
      }
    } else if (input.incrementToken()) {
      String type = typeAtt.type();
      if ((includeInput == null || includeInput.contains(type)) && (excludeInput == null || !excludeInput.contains(type))) {
        state = captureState();
        if (!preserveOriginalType) {
          typeAtt.setType(inputTypeRename);
        }
      }
      return true;
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
      state = null;
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

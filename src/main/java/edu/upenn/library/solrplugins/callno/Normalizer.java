package edu.upenn.library.solrplugins.callno;

/**
 *
 * @author michael
 */
public interface Normalizer {
    /**
     * 
     * @param input
     * @return normalized form, or null if input pattern not recognized
     */
    String normalize(CharSequence input);
    
}

package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.AbstractSolrEventListener;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author michael
 */
public class TopTermOsCacheWarmer extends AbstractSolrEventListener {

  public TopTermOsCacheWarmer(SolrCore core) {
    super(core);
  }
  private static final Logger log = LoggerFactory.getLogger(TopTermOsCacheWarmer.class);
  private static final int DEFAULT_TERM_DICTIONARY_THRESHOLD = 1000;
  private static final int DEFAULT_FREQ_THRESHOLD = 0;
  private static final int DEFAULT_POSITION_THRESHOLD = 0;
  private static final double DEFAULT_THRESHOLD_RATIO = 0.002;
  private static final int AGGREGATE_TERMS_INIT_SIZE = 5000;

  private PriorityQueue<TermStruct> getTopTerms(LeafReader[] segFields, Map<Integer, Map<Integer, Map<Integer, Set<String>>>> parsedArgs, double thresholdRatio, int numTerms) throws IOException {
    Map<BytesRef, TermStruct> termsAggregate = new HashMap<BytesRef, TermStruct>(AGGREGATE_TERMS_INIT_SIZE);
    TermsEnum termsEnum = null;
    int maxFreq = 0;
    int threshold = 0;
    long start = System.currentTimeMillis();
    for (Entry<Integer, Map<Integer, Set<String>>> e0 : parsedArgs.get(WARM_RECON).entrySet()) {
      int freqWarmCount = e0.getKey();
      for (Entry<Integer, Set<String>> e : e0.getValue().entrySet()) {
        int positionWarmCount = e.getKey();
        PostingsEnum postings = null;
        for (String fieldName : e.getValue()) {
          for (LeafReader ar : segFields) {
            Fields f = ar.fields();
            Terms terms = f.terms(fieldName);
            if (terms != null) {
              termsEnum = terms.iterator();
              BytesRef t;
              while ((t = termsEnum.next()) != null) {
                TermStruct ts = termsAggregate.get(t);
                if (ts == null) {
                  int freq = termsEnum.docFreq();
                  if (freq > threshold) {
                    t = stabilize(t);
                    ts = new TermStruct(t, freq);
                    termsAggregate.put(t, ts);
                    if (freq > maxFreq) {
                      maxFreq = freq;
                      threshold = (int)(thresholdRatio * freq);
                    }
                  }
                } else {
                  int freq = ts.add(termsEnum.docFreq());
                  if (freq < threshold) {
                    termsAggregate.remove(t);
                  }
                }
                if (positionWarmCount == WARM_ALL) {
                  warmAllPositions((postings = termsEnum.postings(postings, PostingsEnum.FREQS | PostingsEnum.POSITIONS)), true);
                } else if (freqWarmCount == WARM_ALL && positionWarmCount == 0) {
                  warmAllPositions((postings = termsEnum.postings(postings, PostingsEnum.FREQS)), false);
                }
              }
            }
          }
        }
      }
    }
    log.debug("getTopTerms lookup time: " + (System.currentTimeMillis() - start) + " ms");
    return queueTopTerms(termsAggregate, numTerms);
  }

  private BytesRef stabilize(BytesRef t) {
    byte[] tmp = new byte[t.length];
    System.arraycopy(t.bytes, t.offset, tmp, 0, tmp.length);
    return new BytesRef(tmp);
  }

  private void warmAllPositions(PostingsEnum postings, boolean warmPositions) throws IOException {
    if (postings != null) {
      if (postings.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
        int freq = postings.freq();
        if (warmPositions) {
          do {
            for (int i = 0; i < freq; i++) {
              postings.nextPosition();
            }
          } while (postings.nextDoc() != PostingsEnum.NO_MORE_DOCS && assign(freq = postings.freq()));
        }
      }
    }
  }
  
  private static boolean assign(int val) {
    return true;
  }

  private PriorityQueue<TermStruct> queueTopTerms(Map<BytesRef, TermStruct> termsAggregate, int numTerms) {
    PriorityQueue<TermStruct> topTermQueue = new PriorityQueue<TermStruct>(numTerms, new Comparator<TermStruct>() {
      public int compare(TermStruct o1, TermStruct o2) {
        return o1.freq - o2.freq;
      }
    });
    Iterator<TermStruct> termStructIter = termsAggregate.values().iterator();
    while (termStructIter.hasNext()) {
      TermStruct next = termStructIter.next();
      termStructIter.remove();
      if (topTermQueue.size() < numTerms) {
        topTermQueue.add(next);
      } else if (next.freq > topTermQueue.peek().freq) {
        topTermQueue.remove();
        topTermQueue.add(next);
      }
    }
    return topTermQueue;
  }

  private static List<String> getFieldNames(LeafReader[] segFields) throws IOException {
    Set<String> ret = new HashSet<String>();
    for (LeafReader ar : segFields) {
      Iterator<String> fieldNameIter = ar.fields().iterator();
      while (fieldNameIter.hasNext()) {
        ret.add(fieldNameIter.next());
      }
    }
    return new ArrayList<String>(ret);
  }

  private static class TermGenMetadata {

    private final int numTerms;
    private final double thresholdRatio;

    public TermGenMetadata(int numTerms, double thresholdRatio) {
      this.numTerms = numTerms;
      this.thresholdRatio = thresholdRatio;
    }

  }

  @Override
  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    log.info(TopTermOsCacheWarmer.class.getSimpleName() + " sending requests to " + newSearcher);
    long start = System.currentTimeMillis();
    final SolrIndexSearcher searcher = newSearcher;
    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    LeafReader[] segFields = new LeafReader[leaves.size()];
    Iterator<LeafReaderContext> leafIter = leaves.iterator();
    for (int i = 0; i < segFields.length; i++) {
      segFields[i] = leafIter.next().reader();
    }

    List<NamedList> allLists = (List<NamedList>)getArgs().get("termGenerators");
    if (allLists == null) {
      return;
    }
    for (NamedList termGenParams : allLists) {
      try {
        Map<Integer, Map<Integer, Map<Integer, Set<String>>>> parsedArgs = new HashMap<Integer, Map<Integer, Map<Integer, Set<String>>>>();
        TermGenMetadata tgm = parseTermGenParams(termGenParams, getFieldNames(segFields), parsedArgs);
        if (log.isDebugEnabled()) {
          printParsedArgs(parsedArgs);
        }
        if (testAllOrNothing(parsedArgs)) {
          // we don't need to build the topTerms list.
          allOrNothing(segFields, parsedArgs);
        } else {
          PriorityQueue<TermStruct> topTerms = getTopTerms(segFields, parsedArgs, tgm.thresholdRatio, tgm.numTerms);
          warmTermDictionary(topTerms, segFields, parsedArgs);
        }
      } catch (Exception e) {
        e.printStackTrace(System.out);
      }
    }
    log.info(TopTermOsCacheWarmer.class.getSimpleName() + " done, duration " + ((System.currentTimeMillis() - start) / 1000) + " s");
  }

  private void printParsedArgs(Map<Integer, Map<Integer, Map<Integer, Set<String>>>> parsedArgs) {
    for (Entry<Integer, Map<Integer, Map<Integer, Set<String>>>> e0 : parsedArgs.entrySet()) {
      log.debug(Integer.toString(e0.getKey()));
      for (Entry<Integer, Map<Integer, Set<String>>> e1 : e0.getValue().entrySet()) {
        log.debug('\t' + Integer.toString(e1.getKey()));
        for (Entry<Integer, Set<String>> e2 : e1.getValue().entrySet()) {
          log.debug("\t\t" + e2.getKey() + ", " + e2.getValue());
        }
      }
    }
  }

  private boolean testAllOrNothing(Map<Integer, Map<Integer, Map<Integer, Set<String>>>> parsedArgs) {
    for (Entry<Integer, Map<Integer, Map<Integer, Set<String>>>> e : parsedArgs.entrySet()) {
      if (e.getKey() > 0) {
        return false;
      }
      for (Entry<Integer, Map<Integer, Set<String>>> e1 : e.getValue().entrySet()) {
        if (e1.getKey() > 0) {
          return false;
        }
        for (int posWarmCount : e1.getValue().keySet()) {
          if (posWarmCount > 0) {
            return false;
          }
        }
      }
    }
    return true;
  }

  private void allOrNothing(LeafReader[] segFields, Map<Integer, Map<Integer, Map<Integer, Set<String>>>> parsedArgs) throws IOException {
    log.debug("all or nothing");
    TermsEnum termsEnum = null;
    PostingsEnum postings = null;
    for (Entry<Integer, Map<Integer, Map<Integer, Set<String>>>> e0 : parsedArgs.entrySet()) {
      if (e0.getKey() != 0) {
        for (Entry<Integer, Map<Integer, Set<String>>> e : e0.getValue().entrySet()) {
          if (e.getKey() != 0) {
            for (Entry<Integer, Set<String>> e1 : e.getValue().entrySet()) {
              boolean warmAllPositions = e1.getKey() != 0;
              for (String fieldName : e1.getValue()) {
                for (LeafReader ar : segFields) {
                  Fields f = ar.fields();
                  Terms terms = f.terms(fieldName);
                  if (terms != null) {
                    termsEnum = terms.iterator();
                    while (termsEnum.next() != null) {
                      if (warmAllPositions) {
                        warmAllPositions((postings = termsEnum.postings(postings, PostingsEnum.FREQS | PostingsEnum.POSITIONS)), true);
                      } else {
                        warmAllPositions((postings = termsEnum.postings(postings, PostingsEnum.FREQS)), false);
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  private void debugOutput(TermStruct[] topTerms) {
    for (TermStruct ts : topTerms) {
      log.debug("term: " + ts);
    }
  }

  private void warmTermDictionary(PriorityQueue<TermStruct> topTerms, LeafReader[] segFields, Map<Integer, Map<Integer, Map<Integer, Set<String>>>> blah) throws IOException {
    TermStruct[] termStructs = new TermStruct[topTerms.size()];
    for (int i = 0; i < termStructs.length; i++) {
      termStructs[i] = topTerms.remove();
    }
    if (log.isDebugEnabled()) {
      debugOutput(termStructs);
    }

    for (Entry<Integer, Map<Integer, Map<Integer, Set<String>>>> termDictionaryEntry : blah.entrySet()) {
      int termWarmCount = termDictionaryEntry.getKey();
      for (Entry<Integer, Map<Integer, Set<String>>> freqEntry : termDictionaryEntry.getValue().entrySet()) {
        /*
         * Keep freqWarmCount within bounds; still give priority to the 
         * special count constants (all negative).
         */
        int freqWarmCount = Math.min(freqEntry.getKey(), termStructs.length);
        for (Entry<Integer, Set<String>> positionEntry : freqEntry.getValue().entrySet()) {
          int posWarmCount = Math.min(positionEntry.getKey(), termStructs.length);
          Set<String> seekFields = positionEntry.getValue();
          if (termWarmCount == WARM_RECON && (posWarmCount == WARM_ALL || (freqWarmCount == WARM_ALL && posWarmCount == 0))) {
            // has already been warmed during getTopTerms
          } else if (termWarmCount < 0) {
            if (freqWarmCount <= 0 && posWarmCount <= 0) {
              log.info("warming term all or nothing " + termWarmCount + ", " + freqWarmCount + ", " + posWarmCount + " for " + seekFields);
              warmTermAllOrNothing(segFields, freqWarmCount, posWarmCount, seekFields);
            } else {
              log.info("warming sequentially " + termWarmCount + ", " + freqWarmCount + ", " + posWarmCount + " for " + seekFields);
              warmAllTermsAndPositions(segFields, termStructs, freqWarmCount, posWarmCount, seekFields);
            }
          } else {
            log.info("warming seek " + termWarmCount + ", " + freqWarmCount + ", " + posWarmCount + " for " + seekFields);
            warmTermsAndPositions(segFields, termStructs, termWarmCount, freqWarmCount, posWarmCount, seekFields);
          }
        }
      }
    }

  }

  private void warmTermAllOrNothing(LeafReader[] segFields, int freqWarmCount, int posWarmCount, Set<String> seekFields) throws IOException {
    TermsEnum te = null;
    PostingsEnum postings = null;
    int warmLevel;
    if (freqWarmCount == 0) {
      warmLevel = 0;
    } else if (posWarmCount == 0) {
      warmLevel = 1;
    } else {
      warmLevel = 2;
    }
    for (LeafReader ar : segFields) {
      Fields f = ar.fields();
      for (String seekField : seekFields) {
        Terms terms = f.terms(seekField);
        if (terms != null) {
          te = terms.iterator();
          while (te.next() != null) {
            switch (warmLevel) {
              case 2:
                warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS | PostingsEnum.POSITIONS)), true);
                break;
              case 1:
                warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS)), false);
            }
          }
        }
      }
    }
  }

  private void warmTermsAndPositions(LeafReader[] segFields, TermStruct[] termStructs, int termWarmCount, int freqWarmCount, int posWarmCount, Set<String> seekFields) throws IOException {
    TermsEnum te = null;
    PostingsEnum postings = null;
    for (LeafReader ar : segFields) {
      Fields f = ar.fields();
      for (String seekField : seekFields) {
        Terms terms = f.terms(seekField);
        if (terms != null) {
          te = terms.iterator();
          int termStartIndex = termStructs.length - Math.max(termWarmCount, Math.max(freqWarmCount, posWarmCount));
          int freqStartIndex = (freqWarmCount > 0 ? termStructs.length - freqWarmCount : termStructs.length);
          int posStartIndex = termStructs.length - posWarmCount;
          for (int termIndex = termStartIndex; termIndex < termStructs.length; termIndex++) {
            TermStruct ts = termStructs[termIndex];
            if (te.seekExact(ts.term)) {
              if (termIndex >= posStartIndex) {
                warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS | PostingsEnum.POSITIONS)), true);
              } else if (termIndex >= freqStartIndex) {
                warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS)), false);
              }
            }
          }
        }
      }
    }
  }

  private void warmAllTermsAndPositions(LeafReader[] segFields, TermStruct[] termStructs, int freqWarmCount, int posWarmCount, Set<String> seekFields) throws IOException {
    Set<BytesRef> freqWarmTerms = null;
    Set<BytesRef> posWarmTerms;
    if (posWarmCount == 0) {
      posWarmTerms = Collections.EMPTY_SET;
    } else {
      posWarmTerms = new HashSet<BytesRef>(posWarmCount);
      for (int i = termStructs.length - posWarmCount; i < termStructs.length; i++) {
        posWarmTerms.add(termStructs[i].term);
      }
    }
    if (posWarmCount == freqWarmCount) {
      freqWarmTerms = Collections.EMPTY_SET;
    } else if (freqWarmCount != WARM_ALL) {
      freqWarmTerms = new HashSet<BytesRef>(freqWarmCount - posWarmCount);
      for (int i = termStructs.length - freqWarmCount; i < termStructs.length - posWarmCount; i++) {
        freqWarmTerms.add(termStructs[i].term);
      }
    }
    TermsEnum te = null;
    PostingsEnum postings = null;
    for (LeafReader ar : segFields) {
      Fields f = ar.fields();
      for (String seekField : seekFields) {
        Terms terms = f.terms(seekField);
        if (terms != null) {
          te = terms.iterator();
          BytesRef t;
          while ((t = te.next()) != null) {
            if (posWarmTerms.contains(t)) {
              warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS | PostingsEnum.POSITIONS)), true);
            } else if (freqWarmCount == WARM_ALL || freqWarmTerms.contains(t)) {
              warmAllPositions((postings = te.postings(postings, PostingsEnum.FREQS)), false);
            }
          }
        }
      }
    }
  }

  private class TermStruct {

    private final BytesRef term;
    private int freq;

    public TermStruct(BytesRef term, int freq) {
      this.term = term;
      this.freq = freq;
    }

    public int add(int freq) {
      return this.freq += freq;
    }

    @Override
    public String toString() {
      return "TermStruct{" + "term=\"" + term.utf8ToString() + "\", freq=" + freq + '}';
    }

  }

  private static <T> T getValue(NamedList nl, String key, T defaultValue) {
    int index;
    return ((index = nl.indexOf(key, 0)) < 0 ? defaultValue : (T)nl.getVal(index));
  }

  private static int getStringIntOrConstant(NamedList nl, String key, int defaultValue) {
    int index = nl.indexOf(key, 0);
    if (index < 0) {
      return defaultValue;
    } else {
      String intString = (String)nl.getVal(index);
      if ("all".equals(intString)) {
        return WARM_ALL;
      } else {
        return Integer.parseInt(intString);
      }
    }
  }

  private void addFields(List<String> fields, Map<String, WarmingThresholds> builder, int termDictionaryThreshold, int freqThreshold, int positionThreshold) {
    for (String field : fields) {
      WarmingThresholds wt = builder.get(field);
      if (wt == null) {
        wt = new WarmingThresholds(termDictionaryThreshold, freqThreshold, positionThreshold);
        builder.put(field, wt);
      } else {
        wt.update(termDictionaryThreshold, freqThreshold, positionThreshold);
      }
    }
  }

  private void parseFields(NamedList parent, Map<String, WarmingThresholds> builder, int termDictionaryThreshold, int freqThreshold, int positionThreshold, List<String> defaultFields) {
    List<String> fields = (List<String>)parent.get("fields");
    if (fields != null) {
      addFields(fields, builder, termDictionaryThreshold, freqThreshold, positionThreshold);
    } else if (defaultFields != null) {
      addFields(defaultFields, builder, termDictionaryThreshold, freqThreshold, positionThreshold);
    }
  }

  private double parseReconFields(NamedList termGenParams, Map<String, WarmingThresholds> builder, List<String> fieldNames) {
    int index = termGenParams.indexOf("reconFields", 0);
    if (index < 0) {
      addFields(fieldNames, builder, WARM_RECON, NOT_SET, NOT_SET);
      return DEFAULT_THRESHOLD_RATIO;
    }
    NamedList reconParams = (NamedList)termGenParams.getVal(index);
    int freqThreshold = getStringIntOrConstant(reconParams, "freqThreshold", NOT_SET);
    int positionThreshold = getStringIntOrConstant(reconParams, "positionThreshold", NOT_SET);
    parseFields(reconParams, builder, WARM_RECON, freqThreshold, positionThreshold, fieldNames);
    return getValue(reconParams, "thresholdRatio", DEFAULT_THRESHOLD_RATIO);
  }

  private void parseFieldGroups(NamedList parent, Map<String, WarmingThresholds> builder) {
    for (NamedList fieldGroupParams : (List<NamedList>)parent.getAll("fieldGroup")) {
      int termDictionaryThreshold = getStringIntOrConstant(fieldGroupParams, "termDictionaryThreshold", NOT_SET);
      int freqThreshold = getStringIntOrConstant(fieldGroupParams, "freqThreshold", NOT_SET);
      int positionThreshold = getStringIntOrConstant(fieldGroupParams, "positionThreshold", NOT_SET);
      parseFields(fieldGroupParams, builder, termDictionaryThreshold, freqThreshold, positionThreshold, null);
    }
  }

  private TermGenMetadata parseTermGenParams(NamedList termGenParams, List<String> fieldNames, Map<Integer, Map<Integer, Map<Integer, Set<String>>>> ret) {
    Map<String, WarmingThresholds> builder = new HashMap<String, WarmingThresholds>();
    boolean warmAllFields = getValue(termGenParams, "warmAllFields", false);
    int defaultTermDictionaryThreshold = getStringIntOrConstant(termGenParams, "defaultTermDictionaryThreshold", DEFAULT_TERM_DICTIONARY_THRESHOLD);
    int defaultFreqThreshold = getStringIntOrConstant(termGenParams, "defaultFreqThreshold", DEFAULT_FREQ_THRESHOLD);
    int defaultPositionThreshold = getStringIntOrConstant(termGenParams, "defaultPositionThreshold", DEFAULT_POSITION_THRESHOLD);
    if (warmAllFields) {
      addFields(fieldNames, builder, NOT_SET, NOT_SET, NOT_SET);
    }
    double thresholdRatio = parseReconFields(termGenParams, builder, fieldNames);
    parseFieldGroups(termGenParams, builder);
    int maxNumTerms = 0;
    for (Entry<String, WarmingThresholds> e : builder.entrySet()) {
      String fieldName = e.getKey();
      WarmingThresholds wt = e.getValue();
      wt.registerDefaults(defaultTermDictionaryThreshold, defaultFreqThreshold, defaultPositionThreshold);
      int termThreshold = wt.getTermThreshold();
      Map<Integer, Map<Integer, Set<String>>> freqMap = ret.get(termThreshold);
      if (freqMap == null) {
        freqMap = new HashMap<Integer, Map<Integer, Set<String>>>();
        ret.put(termThreshold, freqMap);
        if (termThreshold > maxNumTerms) {
          maxNumTerms = termThreshold;
        }
      }
      int freqThreshold = wt.getFreqThreshold();
      Map<Integer, Set<String>> posMap = freqMap.get(freqThreshold);
      if (posMap == null) {
        posMap = new HashMap<Integer, Set<String>>();
        freqMap.put(freqThreshold, posMap);
        if (freqThreshold > maxNumTerms) {
          maxNumTerms = freqThreshold;
        }
      }
      int positionThreshold = wt.getPositionThreshold();
      Set<String> targetFields = posMap.get(positionThreshold);
      if (targetFields == null) {
        targetFields = new HashSet<String>();
        posMap.put(positionThreshold, targetFields);
        if (positionThreshold > maxNumTerms) {
          maxNumTerms = positionThreshold;
        }
      }
      targetFields.add(fieldName);
    }
    return new TermGenMetadata(maxNumTerms, thresholdRatio);
  }

  private static final int NOT_SET = -1;
  private static final int WARM_ALL = -2;
  private static final int WARM_RECON = -3;

  private class WarmingThresholds {

    private int term;
    private int freq;
    private int position;

    private WarmingThresholds(int term, int freq, int position) {
      if (term >= 0) {
        term = Integer.MAX_VALUE - term;
      }
      if (freq >= 0) {
        freq = Integer.MAX_VALUE - freq;
      }
      if (position >= 0) {
        position = Integer.MAX_VALUE - position;
      }
      this.term = term;
      this.freq = freq;
      this.position = position;
    }

    private void update(int term, int freq, int position) {
      if (term >= 0) {
        term = Integer.MAX_VALUE - term;
      }
      if (freq >= 0) {
        freq = Integer.MAX_VALUE - freq;
      }
      if (position >= 0) {
        position = Integer.MAX_VALUE - position;
      }
      if (term < this.term && term != NOT_SET || this.term == NOT_SET) {
        this.term = term;
      }
      if (freq < this.freq && freq != NOT_SET || this.freq == NOT_SET) {
        this.freq = freq;
      }
      if (position < this.position && position != NOT_SET || this.position == NOT_SET) {
        this.position = position;
      }
    }

    private void registerDefaults(int defaultTerm, int defaultFreq, int defaultPosition) {
      if (defaultTerm >= 0) {
        defaultTerm = Integer.MAX_VALUE - defaultTerm;
      }
      if (defaultFreq >= 0) {
        defaultFreq = Integer.MAX_VALUE - defaultFreq;
      }
      if (defaultPosition >= 0) {
        defaultPosition = Integer.MAX_VALUE - defaultPosition;
      }
      if (position == NOT_SET) {
        position = defaultPosition;
      }
      if (freq == NOT_SET) {
        freq = defaultFreq;
      }
      if (term == NOT_SET) {
        term = defaultTerm;
      }
      if (position < freq) {
        freq = position;
      }
      if (freq < term) {
        term = freq;
      }
    }

    private int getTermThreshold() {
      return (term >= 0 ? Integer.MAX_VALUE - term : term);
    }

    private int getFreqThreshold() {
      return (freq >= 0 ? Integer.MAX_VALUE - freq : freq);
    }

    private int getPositionThreshold() {
      return (position >= 0 ? Integer.MAX_VALUE - position : position);
    }
  }

}

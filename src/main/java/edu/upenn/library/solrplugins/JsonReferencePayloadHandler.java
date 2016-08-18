package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.FacetPayload;

/**
 * Builds facet payloads for fields tokenized by JsonReferencePayloadTokenizer.
 *
 * @author jeffchiu
 */
public class JsonReferencePayloadHandler implements FacetPayload<NamedList<Object>> {

  @Override
  public boolean addEntry(String termKey, int count, PostingsEnum postings, NamedList res) throws IOException {
    res.add(termKey, buildEntryValue(count, postings));
    return true;
  }

  @Override
  public Map.Entry<String, NamedList<Object>> addEntry(String termKey, int count, PostingsEnum postings) throws IOException {
    return new AbstractMap.SimpleImmutableEntry<>(termKey, buildEntryValue(count, postings));
  }

  private NamedList<Object> buildEntryValue(int count, PostingsEnum postings) throws IOException {
    // proof-of-concept implementation
    NamedList<Object> entry = new NamedList<>();

    // document count for this term
    entry.add("count", count);

    // nested map: intermediate data structure used for counting within this block
    Map<String, Map<String, Integer>> referenceTypesToTargetCounts = null;

    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      for (int j = 0; j < postings.freq(); j++) {
        postings.nextPosition();

        BytesRef payload = postings.getPayload();
        if (payload != null) {
          referenceTypesToTargetCounts = new HashMap<>();

          String payloadStr = payload.utf8ToString();
          int pos = payloadStr.indexOf(JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR);
          if (pos != -1) {
            String referenceType = payloadStr.substring(0, pos);
            String target = payloadStr.substring(pos + 1);

            Map<String, Integer> targetsToCounts;
            if (!referenceTypesToTargetCounts.containsKey(referenceType)) {
              targetsToCounts = new HashMap<>();
              referenceTypesToTargetCounts.put(referenceType, targetsToCounts);
            } else {
              targetsToCounts = referenceTypesToTargetCounts.get(referenceType);
            }

            int newCount = targetsToCounts.getOrDefault(target, 0) + 1;
            targetsToCounts.put(target, newCount);
          }
        }

        // Couldn't get this to work: postings.attributes() doesn't return anything: why?
        /*
        ReferenceAttribute refAtt = postings.attributes().getAttribute(ReferenceAttribute.class);
        if(refAtt != null) {
          System.out.println("found refAttr, " + refAtt.getReferenceType() + "," + refAtt.getTarget());
        }
        */
      }
    }

    // convert referenceTypesToTargetCounts to NamedList pairs expected by Solr
    if (referenceTypesToTargetCounts != null) {
      for (String referenceType : referenceTypesToTargetCounts.keySet()) {
        NamedList<Object> targetCountPairs = new NamedList<>();
        Map<String, Integer> targetsToCounts = referenceTypesToTargetCounts.get(referenceType);
        for (String target : targetsToCounts.keySet()) {
          targetCountPairs.add(target, targetsToCounts.get(target));
        }
        entry.add(referenceType, targetCountPairs);
      }
    }

    return entry;
  }

  @Override
  public NamedList<Object> mergePayload(NamedList<Object> preExisting, NamedList<Object> add, long preExistingCount, long addCount) {

    if (addCount != ((Number)add.remove("count")).longValue()) {
      throw new IllegalStateException("fieldType-internal and -external counts do not match");
    }
    int countIndex = preExisting.indexOf("count", 0);
    long preCount = ((Number)preExisting.getVal(countIndex)).longValue();
    preExisting.setVal(countIndex, preCount + addCount);

    Iterator<Map.Entry<String, Object>> refTypesIter = add.iterator();
    while (refTypesIter.hasNext()) {
      Map.Entry<String, Object> entry = refTypesIter.next();
      String addReferenceType = entry.getKey();
      NamedList<Object> addTargetCounts = (NamedList<Object>) entry.getValue();

      // if this referenceType doesn't exist in preExisting yet, create it
      NamedList<Object> existingTargetCounts = (NamedList<Object>) preExisting.get(addReferenceType);
      if (existingTargetCounts == null) {
        existingTargetCounts = new NamedList<Object>();
        preExisting.add(addReferenceType, existingTargetCounts);
      }

      // loop through target+count pairs, merge them into preExisting
      Iterator<Map.Entry<String, Object>> addTargetCountsIter = addTargetCounts.iterator();
      while (addTargetCountsIter.hasNext()) {
        Map.Entry<String, Object> targetCountEntry = addTargetCountsIter.next();
        String target = targetCountEntry.getKey();
        Number addTargetCount = (Number) targetCountEntry.getValue();

        int index = existingTargetCounts.indexOf(target, 0);
        long existingCount = 0;
        Number existingCountNum = (Number) existingTargetCounts.get(target);
        if (existingCountNum != null) {
          existingCount = existingCountNum.longValue();
        }
        existingCount += addTargetCount.longValue();
        if (index != -1) {
          existingTargetCounts.setVal(index, existingCount);
        } else {
          existingTargetCounts.add(target, existingCount);
        }
      }
    }

    return preExisting;
  }

  @Override
  public long extractCount(NamedList<Object> val) {
    return ((Number) val.get("count")).longValue();
  }

  @Override
  public Object updateValueExternalRepresentation(NamedList<Object> internal) {
    return null;
  }

}

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
 * Builds facet payloads from fields containing normalized, filing, and
 * prefix strings joined by a delimiter, and a payload attribute
 * containing information about references.
 *
 * The NamedList structure for a facet will look like this:
 *
 * <lst name="subject_xfacet">
 *   <lst name="Hegelianism">
 *     <int name="count">3</int>
 *     <str name="prefix"></str>
 *     <str name="filing">Hegelianism</str>
 *     <lst name="refs">
 *       <lst name="see_also">
 *         <long name="Georg Wilhelm Friedrich Hegel">1</long>
 *         <long name="History of Marxism">1</long>
 *       </lst>
 *     </lst>
 *   </lst>
 *   <lst name="Georg Wilhelm Friedrich Hegel">
 *     <int name="count">2</int>
 *     <str name="prefix">Georg Wilhelm Friedrich </str>
 *     <str name="filing">Hegel</str>
 *     <lst name="refs">
 *       <lst name="see_also">
 *         <long name="History of Marxism">1</long>
 *       </lst>
 *     </lst>
 *   </lst>
 * </lst>
 *
 * @author jeffchiu
 */
public class JsonReferencePayloadHandler implements FacetPayload<NamedList<Object>> {
  private static final String DELIM = "\u0000";
  private static final String KEY_REFS = "refs";
  private static final String KEY_PREFIX = "prefix";
  private static final String KEY_FILING = "filing";

  @Override
  public boolean addEntry(String termKey, int count, PostingsEnum postings, NamedList res) throws IOException {
    String[] parts = termKey.split(DELIM);
    if(parts.length < 2) {
      throw new IOException("Expected term '" + termKey + "' to split into at least 2 strings, but split resulted in " + parts.length + " strings instead");
    }
    String normalized = parts[0];
    String filing = parts[1];
    String prefix = "";
    if(parts.length > 2) {
      prefix = parts[2];
    }

    NamedList<Object> entry = buildEntryValue(count, postings);
    entry.add(KEY_PREFIX, prefix);
    entry.add(KEY_FILING, filing);

    res.add(prefix + filing, entry);
    return true;
  }

  @Override
  public Map.Entry<String, NamedList<Object>> addEntry(String termKey, int count, PostingsEnum postings) throws IOException {
    return new AbstractMap.SimpleImmutableEntry<>(termKey, buildEntryValue(count, postings));
  }

  private NamedList<Object> buildEntryValue(int count, PostingsEnum postings) throws IOException {
    NamedList<Object> entry = new NamedList<>();

    // document count for this term
    entry.add("count", count);

    NamedList<Object> refs = new NamedList<>();

    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      for (int j = 0; j < postings.freq(); j++) {
        postings.nextPosition();

        BytesRef payload = postings.getPayload();
        if (payload != null) {
          String payloadStr = payload.utf8ToString();
          int pos = payloadStr.indexOf(JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR);
          if (pos != -1) {
            String referenceType = payloadStr.substring(0, pos);
            String target = payloadStr.substring(pos + 1);

            NamedList<Object> targetCountPairs = (NamedList<Object>) entry.get(referenceType);
            if(targetCountPairs == null) {
              targetCountPairs = new NamedList<>();
              refs.add(referenceType, targetCountPairs);
            }

            int indexOfTarget = targetCountPairs.indexOf(target, 0);
            if(indexOfTarget != -1) {
              long oldCount = ((Number) targetCountPairs.getVal(indexOfTarget)).longValue();
              targetCountPairs.setVal(indexOfTarget, oldCount + 1L);
            } else {
              targetCountPairs.add(target, 1L);
            }
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

    if(refs.size() > 0) {
      entry.add(KEY_REFS, refs);
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

    if(add.get(KEY_REFS) != null) {
      NamedList<Object> addRefs = (NamedList<Object>) add.get(KEY_REFS);
      Iterator<Map.Entry<String, Object>> refTypesIter = addRefs.iterator();
      while (refTypesIter.hasNext()) {
        Map.Entry<String, Object> entry = refTypesIter.next();
        String addReferenceType = entry.getKey();
        NamedList<Object> addTargetCounts = (NamedList<Object>) entry.getValue();

        // if this referenceType doesn't exist in preExisting yet, create it
        NamedList<Object> preExistingRefs = (NamedList<Object>) preExisting.get(KEY_REFS);
        if(preExistingRefs == null) {
          preExistingRefs = new NamedList<Object>();
          preExisting.add(KEY_REFS, preExistingRefs);
        }
        NamedList<Object> existingTargetCounts = (NamedList<Object>) preExistingRefs.get(addReferenceType);
        if (existingTargetCounts == null) {
          existingTargetCounts = new NamedList<Object>();
        }
        preExistingRefs.add(addReferenceType, existingTargetCounts);

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

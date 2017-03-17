package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.FacetPayload;

/**
 * Builds facet payloads from fields containing filing and
 * prefix strings joined by a delimiter, and a payload attribute
 * containing information about references.
 *
 * The NamedList structure for a facet will look like this:
 *
 * <lst name="subject_xfacet">
 *   <lst name="Hegelianism">
 *     <int name="count">3</int>
 *     <lst name="self">
 *       <long name="count">2</long>
 *       <str name="filing">Hegelianism</str>
 *     </lst>
 *     <lst name="refs">
 *       <lst name="see_also">
 *         <lst name="Georg Wilhelm Friedrich Hegel">
 *           <long name="count">1</int>
 *           <str name="prefix">Georg Wilhelm Friedrich </str>
 *           <str name="filing">Hegel</str>
 *         </lst>
 *       </lst>
 *     </lst>
 *   </lst>
 * </lst>
 *
 * @author jeffchiu
 */
public class JsonReferencePayloadHandler implements FacetPayload<NamedList<Object>> {
  private static final String DELIM = "\u0000";
  private static final String KEY_SELF = "self";
  private static final String KEY_REFS = "refs";
  private static final String KEY_PREFIX = "prefix";
  private static final String KEY_FILING = "filing";
  private static final String KEY_COUNT = "count";

  /**
   * overwrite entry in NamedList with new value
   * (update existing key, or add the key/value if key doesn't already exist)
   */
  private static void overwriteInNamedList(NamedList<Object> namedList, String key, Object value) {
    int indexOfKey = namedList.indexOf(key, 0);
    if(indexOfKey != -1) {
      namedList.setVal(indexOfKey, value);
    } else {
      namedList.add(key, value);
    }
  }

  /**
   * Copies a field value from one NamedList into another
   */
  private static void copyFieldInNamedList(NamedList<Object> from, NamedList<Object> to, String key) {
    int index = from.indexOf(key, 0);
    if(index != -1) {
      Object value = from.get(key);
      int index2 = to.indexOf(key, 0);
      if(index2 != -1) {
        to.setVal(index2, value);
      } else {
        to.add(key, value);
      }
    }
  }

  /**
   * For passed-in NamedList, get the NamedList value for a certain key,
   * creating and storing it if it doesn't exist.
   * @param namedList
   * @param key
   */
  private static NamedList<Object> getOrCreateNamedListValue(NamedList<Object> namedList, String key) {
    NamedList<Object> result = (NamedList<Object>) namedList.get(key);
    if(result == null) {
      result = new NamedList<>();
      namedList.add(key, result);
    }
    return result;
  }

  /**
   * increment a Long value in a NamedList stored under "key", creating it with value of 1
   * if it doesn't exist.
   */
  private static void incrementLongInNamedList(NamedList<Object> namedList, String key) {
    int index = namedList.indexOf(key, 0);
    if(index != -1) {
      long oldCount = ((Number) namedList.getVal(index)).longValue();
      namedList.setVal(index, oldCount + 1L);
    } else {
      namedList.add(key, 1L);
    }
  }

  /**
   * Updates the Long value for the specified key in the 'preExisting' NamedList
   * by adding the value from the 'add' NamedList.
   */
  private static void mergeCount(NamedList<Object> from, NamedList<Object> to, String key) {
    // merge count
    long existingCount = 0;
    int indexOfCount = to.indexOf(key, 0);
    if(indexOfCount != -1) {
      existingCount = ((Number) to.get(key)).longValue();
    }
    long newCount = existingCount + ((Number) from.get(key)).longValue();
    overwriteInNamedList(to, key, newCount);
  }

  @Override
  public boolean addEntry(String termKey, long count, PostingsEnum postings, Bits liveDocs, NamedList res) throws IOException {
    MultiPartString term = MultiPartString.parseNormalizedFilingAndPrefix(termKey);

    NamedList<Object> entry = buildEntryValue(term, count, postings, liveDocs);

    res.add(term.getDisplay(), entry);
    return true;
  }

  @Override
  public Map.Entry<String, NamedList<Object>> addEntry(String termKey, long count, PostingsEnum postings, Bits liveDocs) throws IOException {
    MultiPartString term = MultiPartString.parseNormalizedFilingAndPrefix(termKey);
    return new AbstractMap.SimpleImmutableEntry<>(termKey, buildEntryValue(term, count, postings, liveDocs));
  }

  private NamedList<Object> buildEntryValue(MultiPartString term, long count, PostingsEnum postings, Bits liveDocs) throws IOException {
    NamedList<Object> entry = new NamedList<>();

    // document count for this term
    entry.add(KEY_COUNT, count);

    NamedList<Object> self = new NamedList<>();
    entry.add(KEY_SELF, self);

    self.add(KEY_COUNT, 0L);
    overwriteInNamedList(self, KEY_FILING, term.getFiling());
    if(term.getPrefix() != null) {
      overwriteInNamedList(self, KEY_PREFIX, term.getPrefix());
    }

    NamedList<Object> refs = new NamedList<>();

    while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      if (liveDocs != null && !liveDocs.get(postings.docID())) {
        continue;
      }
      for (int j = 0; j < postings.freq(); j++) {
        postings.nextPosition();

        BytesRef payload = postings.getPayload();
        if (payload != null) {
          String payloadStr = payload.utf8ToString();
          int pos = payloadStr.indexOf(JsonReferencePayloadTokenizer.PAYLOAD_ATTR_SEPARATOR);
          if (pos != -1) {
            String referenceType = payloadStr.substring(0, pos);
            String target = payloadStr.substring(pos + 1);

            MultiPartString multiPartString = MultiPartString.parseFilingAndPrefix(target);
            String displayName = multiPartString.getDisplay();

            NamedList<Object> displayNameStructs = getOrCreateNamedListValue(refs, referenceType);

            NamedList<Object> nameStruct = getOrCreateNamedListValue(displayNameStructs, displayName);

            incrementLongInNamedList(nameStruct, KEY_COUNT);

            overwriteInNamedList(nameStruct, KEY_FILING, multiPartString.getFiling());
            if(multiPartString.getPrefix() != null) {
              overwriteInNamedList(nameStruct, KEY_PREFIX, multiPartString.getPrefix());
            }
          }
        } else {
          // no payload means term is for self, so increment count
          incrementLongInNamedList(self, KEY_COUNT);
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

    if (addCount != ((Number)add.get(KEY_COUNT)).longValue()) {
      throw new IllegalStateException("fieldType-internal and -external counts do not match");
    }
    int countIndex = preExisting.indexOf(KEY_COUNT, 0);
    long preCount = ((Number)preExisting.getVal(countIndex)).longValue();
    preExisting.setVal(countIndex, preCount + addCount);

    if(add.get(KEY_SELF) != null) {
      NamedList<Object> addSelf = (NamedList<Object>) add.get(KEY_SELF);

      NamedList<Object> preExistingSelf = getOrCreateNamedListValue(preExisting, KEY_SELF);

      mergeCount(addSelf, preExistingSelf, KEY_COUNT);

      copyFieldInNamedList(addSelf, preExistingSelf, KEY_FILING);
      copyFieldInNamedList(addSelf, preExistingSelf, KEY_PREFIX);
    }

    if(add.get(KEY_REFS) != null) {
      NamedList<Object> addRefs = (NamedList<Object>) add.get(KEY_REFS);
      Iterator<Map.Entry<String, Object>> refTypesIter = addRefs.iterator();
      while (refTypesIter.hasNext()) {
        Map.Entry<String, Object> entry = refTypesIter.next();
        String addReferenceType = entry.getKey();
        NamedList<Object> addNameStructs = (NamedList<Object>) entry.getValue();

        // if "refs" doesn't exist in preExisting yet, create it
        NamedList<Object> preExistingRefs = getOrCreateNamedListValue(preExisting, KEY_REFS);

        // if referenceType doesn't exist in preExisting yet, create it
        NamedList<Object> preExistingNameStructs = getOrCreateNamedListValue(preExistingRefs, addReferenceType);

        // loop through names and merge them into preExisting
        Iterator<Map.Entry<String, Object>> addNameStructsIter = addNameStructs.iterator();
        while (addNameStructsIter.hasNext()) {
          Map.Entry<String, Object> nameStructEntry = addNameStructsIter.next();
          String name = nameStructEntry.getKey();
          NamedList<Object> addNameStruct = (NamedList<Object>) nameStructEntry.getValue();

          // if name doesn't exist in preExisting yet, create it
          NamedList<Object> preExistingNameStruct = getOrCreateNamedListValue(preExistingNameStructs, name);

          mergeCount(addNameStruct, preExistingNameStruct, KEY_COUNT);

          copyFieldInNamedList(addNameStruct, preExistingNameStruct, KEY_FILING);
          copyFieldInNamedList(addNameStruct, preExistingNameStruct, KEY_PREFIX);
        }
      }
    }
    return preExisting;
  }

  @Override
  public long extractCount(NamedList<Object> val) {
    return ((Number) val.get(KEY_COUNT)).longValue();
  }

  @Override
  public Object updateValueExternalRepresentation(NamedList<Object> internal) {
    return null;
  }

}

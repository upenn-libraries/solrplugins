package edu.upenn.library.solrplugins;

import java.io.IOException;
import java.util.AbstractMap;
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
 *     <str name="normalized">hegelianism</str>
 *     <str name="prefix"></str>
 *     <str name="filing">Hegelianism</str>
 *     <lst name="refs">
 *       <lst name="see_also">
 *         <lst name="Georg Wilhelm Friedrich Hegel">
 *           <long name="count">1</int>
 *           <str name="normalized">hegel</str>
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
  private static final String KEY_REFS = "refs";
  private static final String KEY_NORMALIZED = "normalized";
  private static final String KEY_PREFIX = "prefix";
  private static final String KEY_FILING = "filing";

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
  private static void copyField(NamedList<Object> from, NamedList<Object> to, String key) {
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

  @Override
  public boolean addEntry(String termKey, int count, PostingsEnum postings, NamedList res) throws IOException {
    MultiPartString multiPartString = MultiPartString.parse(termKey);

    NamedList<Object> entry = buildEntryValue(count, postings);
    entry.add(KEY_NORMALIZED, multiPartString.getNormalized());
    entry.add(KEY_FILING, multiPartString.getFiling());
    if(multiPartString.getPrefix() != null) {
      entry.add(KEY_PREFIX, multiPartString.getPrefix());
    }

    res.add(multiPartString.getDisplay(), entry);
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

            MultiPartString multiPartString = MultiPartString.parse(target);
            String displayName = multiPartString.getDisplay();

            NamedList<Object> displayNameStructs = (NamedList<Object>) entry.get(referenceType);
            if(displayNameStructs == null) {
              displayNameStructs = new NamedList<>();
              refs.add(referenceType, displayNameStructs);
            }

            NamedList<Object> nameStruct = (NamedList<Object>) displayNameStructs.get(displayName);
            if(nameStruct == null) {
              nameStruct = new NamedList<>();
              displayNameStructs.add(displayName, nameStruct);
            }

            int indexOfCount = nameStruct.indexOf("count", 0);
            if(indexOfCount != -1) {
              long oldCount = ((Number) nameStruct.getVal(indexOfCount)).longValue();
              nameStruct.setVal(indexOfCount, oldCount + 1L);
            } else {
              nameStruct.add("count", 1L);
            }

            overwriteInNamedList(nameStruct, "normalized", multiPartString.getNormalized());
            overwriteInNamedList(nameStruct, "filing", multiPartString.getFiling());
            if(multiPartString.getPrefix() != null) {
              overwriteInNamedList(nameStruct, "prefix", multiPartString.getPrefix());
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
        NamedList<Object> addNameStructs = (NamedList<Object>) entry.getValue();

        // if "refs" doesn't exist in preExisting yet, create it
        NamedList<Object> preExistingRefs = (NamedList<Object>) preExisting.get(KEY_REFS);
        if(preExistingRefs == null) {
          preExistingRefs = new NamedList<Object>();
          preExisting.add(KEY_REFS, preExistingRefs);
        }
        // if referenceType doesn't exist in preExisting yet, create it
        NamedList<Object> preExistingNameStructs = (NamedList<Object>) preExistingRefs.get(addReferenceType);
        if (preExistingNameStructs == null) {
          preExistingNameStructs = new NamedList<Object>();
        }
        preExistingRefs.add(addReferenceType, preExistingNameStructs);

        // loop through names and merge them into preExisting
        Iterator<Map.Entry<String, Object>> addNameStructsIter = addNameStructs.iterator();
        while (addNameStructsIter.hasNext()) {
          Map.Entry<String, Object> nameStructEntry = addNameStructsIter.next();
          String name = nameStructEntry.getKey();
          NamedList<Object> addNameStruct = (NamedList<Object>) nameStructEntry.getValue();

          // if name doesn't exist in preExisting yet, create it
          int index = preExistingNameStructs.indexOf(name, 0);
          NamedList<Object> preExistingNameStruct;
          if (index != -1) {
            preExistingNameStruct = (NamedList<Object>) preExistingNameStructs.getVal(index);
          } else {
            preExistingNameStruct = new NamedList<Object>();
            preExistingNameStructs.add(name, preExistingNameStruct);
          }

          // merge count
          long existingCount = 0;
          int indexOfCount = preExistingNameStruct.indexOf("count", 0);
          if(indexOfCount != -1) {
            existingCount = ((Number) preExistingNameStruct.get("count")).longValue();
          }
          long newCount = existingCount + ((Number) addNameStruct.get("count")).longValue();
          if(indexOfCount != -1) {
            preExistingNameStruct.setVal(indexOfCount, newCount);
          } else {
            preExistingNameStruct.add("count", newCount);
          }

          copyField(addNameStruct, preExistingNameStruct, "normalized");
          copyField(addNameStruct, preExistingNameStruct, "filing");
          copyField(addNameStruct, preExistingNameStruct, "prefix");
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

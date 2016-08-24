package edu.upenn.library.solrplugins;

import static junit.framework.Assert.assertEquals;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class JsonReferencePayloadHandlerTest {

  @Test
  public void testMergePayload() {
    NamedList<Object> preUseForTargetCounts = new NamedList<>();
    preUseForTargetCounts.add("G. Hegel", 2L);
    preUseForTargetCounts.add("Georg Hegel", 3L);

    NamedList<Object> preRefs = new NamedList<>();
    preRefs.add("use_for", preUseForTargetCounts);

    NamedList<Object> preExisting = new NamedList<>();
    preExisting.add("count", 10L);
    preExisting.add("refs", preRefs);

    NamedList<Object> addUseForTargetCounts = new NamedList<>();
    addUseForTargetCounts.add("G. Hegel", 4L);
    addUseForTargetCounts.add("Hegel", 1L);

    NamedList<Object> addSeeAlsoTargetCounts = new NamedList<>();
    addSeeAlsoTargetCounts.add("G. W. F. Hegel", 4L);

    NamedList<Object> addRefs = new NamedList<>();
    addRefs.add("use_for", addUseForTargetCounts);
    addRefs.add("see_also", addSeeAlsoTargetCounts);

    NamedList<Object> add = new NamedList<>();
    add.add("count", 4L);
    add.add("refs", addRefs);

    JsonReferencePayloadHandler handler = new JsonReferencePayloadHandler();

    NamedList<Object> result = (NamedList<Object>) handler.mergePayload(preExisting, add, 10L, 4L);

    assertEquals(14L, result.get("count"));

    NamedList<Object> mergedRefs = (NamedList<Object>) result.get("refs");
    NamedList<Object> useForTargetCounts = (NamedList<Object>) mergedRefs.get("use_for");

    assertEquals(3, useForTargetCounts.size());

    assertEquals(6L, useForTargetCounts.get("G. Hegel"));
    assertEquals(3L, useForTargetCounts.get("Georg Hegel"));
    assertEquals(1L, useForTargetCounts.get("Hegel"));

    NamedList<Object> seeAlsoTargetCounts = (NamedList<Object>) mergedRefs.get("see_also");

    assertEquals(1, seeAlsoTargetCounts.size());

    assertEquals(4L, seeAlsoTargetCounts.get("G. W. F. Hegel"));
  }

}

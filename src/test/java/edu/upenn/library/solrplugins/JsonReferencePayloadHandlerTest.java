package edu.upenn.library.solrplugins;

import static junit.framework.Assert.assertEquals;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

public class JsonReferencePayloadHandlerTest {

  @Test
  public void testMergePayload() {
    // build data for preExisting record

    NamedList<Object> preGHegelStruct = new NamedList<>();
    preGHegelStruct.add("count", 2L);
    preGHegelStruct.add("prefix", "G. ");
    preGHegelStruct.add("filing", "Hegel");

    NamedList<Object> preGeorgHegelStruct = new NamedList<>();
    preGeorgHegelStruct.add("count", 3L);
    preGeorgHegelStruct.add("prefix", "Georg ");
    preGeorgHegelStruct.add("filing", "Hegel2");

    NamedList<Object> preUseForNameStructs = new NamedList<>();
    preUseForNameStructs.add("G. Hegel", preGHegelStruct);
    preUseForNameStructs.add("Georg Hegel2", preGeorgHegelStruct);

    NamedList<Object> preRefs = new NamedList<>();
    preRefs.add("use_for", preUseForNameStructs);

    NamedList<Object> preSelf = new NamedList<>();
    preSelf.add("count", 4L);
    preSelf.add("filing", "Hegel");

    NamedList<Object> preExisting = new NamedList<>();
    preExisting.add("count", 9L);
    preExisting.add("refs", preRefs);
    preExisting.add("self", preSelf);

    // build data for record to add/merge

    NamedList<Object> addGHegelStruct = new NamedList<>();
    addGHegelStruct.add("count", 4L);
    addGHegelStruct.add("prefix", "G. ");
    addGHegelStruct.add("filing", "Hegel");

    NamedList<Object> addHegelStruct = new NamedList<>();
    addHegelStruct.add("count", 1L);
    addHegelStruct.add("filing", "Hegel3");

    NamedList<Object> addUseForNameStructs = new NamedList<>();
    addUseForNameStructs.add("G. Hegel", addGHegelStruct);
    addUseForNameStructs.add("Hegel3", addHegelStruct);

    NamedList<Object> addGWFHegelStruct = new NamedList<>();
    addGWFHegelStruct.add("count", 4L);
    addGWFHegelStruct.add("prefix", "G. W. F. ");
    addGWFHegelStruct.add("filing", "Hegel");

    NamedList<Object> addSeeAlsoNameStructs = new NamedList<>();
    addSeeAlsoNameStructs.add("G. W. F. Hegel", addGWFHegelStruct);

    NamedList<Object> addRefs = new NamedList<>();
    addRefs.add("use_for", addUseForNameStructs);
    addRefs.add("see_also", addSeeAlsoNameStructs);

    NamedList<Object> addSelf = new NamedList<>();
    addSelf.add("count", 3L);
    addSelf.add("filing", "Hegel");

    NamedList<Object> add = new NamedList<>();
    add.add("count", 12L);
    add.add("refs", addRefs);
    add.add("self", addSelf);

    // test merge

    JsonReferencePayloadHandler handler = new JsonReferencePayloadHandler();

    NamedList<Object> result = (NamedList<Object>) handler.mergePayload(preExisting, add, 9L, 12L);

    assertEquals(21L, result.get("count"));

    NamedList<Object> self = (NamedList<Object>) result.get("self");
    assertEquals(7L, self.get("count"));
    assertEquals("Hegel", self.get("filing"));

    NamedList<Object> mergedRefs = (NamedList<Object>) result.get("refs");
    NamedList<Object> useForNameStructs = (NamedList<Object>) mergedRefs.get("use_for");

    assertEquals(3, useForNameStructs.size());

    NamedList<Object> useFor1 = (NamedList<Object>) useForNameStructs.get("G. Hegel");
    assertEquals(6L, useFor1.get("count"));
    assertEquals("G. ", useFor1.get("prefix"));
    assertEquals("Hegel", useFor1.get("filing"));

    NamedList<Object> useFor2 = (NamedList<Object>) useForNameStructs.get("Georg Hegel2");
    assertEquals(3L, useFor2.get("count"));
    assertEquals("Georg ", useFor2.get("prefix"));
    assertEquals("Hegel2", useFor2.get("filing"));

    NamedList<Object> useFor3 = (NamedList<Object>) useForNameStructs.get("Hegel3");
    assertEquals(1L, useFor3.get("count"));
    assertEquals("Hegel3", useFor3.get("filing"));

    NamedList<Object> seeAlsoNameStructs = (NamedList<Object>) mergedRefs.get("see_also");

    assertEquals(1, seeAlsoNameStructs.size());

    NamedList<Object> seeAlso1 = (NamedList<Object>) seeAlsoNameStructs.get("G. W. F. Hegel");

    assertEquals(4L, seeAlso1.get("count"));
    assertEquals("G. W. F. ", seeAlso1.get("prefix"));
    assertEquals("Hegel", seeAlso1.get("filing"));
  }

}

# Solr Plugins to support normalized term browse with term-based cross-references

## How is the project structured and updated?

The approach to extending Solr code is slightly unorthodox. 
A branch 'solr-upstream-base' is maintained with stock versions
of tagged releases for solr classes to be modified. A script
is maintained in that branch that specifies the solr files to
be extended, and is responsible for downloading them into the
branch (to achieve a sort of pseudo-remote-tracking branch).

This branch can then be merged (one-way) into the master branch,
allowing a smooth path to keep modifications up-to-date with
the base implementation from Solr. 

## `edu.upenn.library.solrplugins.tokentype` package

The approach to normalized sorting of terms is accomplished by
having the indexed value be the raw input, prepended by a 
normalized (case-folded, etc.) version of the raw input, delimited
by a null byte. 

This could be achieved external to Solr (with a stock Solr
implementation) by pre-processing fields in Solr input/update docs 
to be constructed in this way, and processing on client-side at 
query time to extract the raw (display) value from the 
normalized-sortable indexed value. This is not ideal, for two 
key reasons:
1. Transfering content including null bytes has many potential 
pitfalls in practice
2. Solr has standardized, highly-customizable normalization 
and token analysis capabilities built-in.

In order to avoid point 1 and take advantage of point 2, the 
classes in the `tokentype` package take advantage of Lucene token
types as defined in the `org.apache.lucene.analysis.tokenattributes.TypeAttribute`
class to fork tokens (assigning different token types to each fork), 
selectively process tokens according to type (delegating the actual
processing to standard solr/lucene analyzers), and merge/concatenate
adjacent tokens into single output tokens.

```xml
<fieldType name="normSortTerm" class="[package].CaseInsensitiveSortingTextField">
  <analyzer>
    <tokenizer class="solr.KeywordTokenizerFactory"/>
    <filter class="[tokentype].TokenTypeSplitFilterFactory" inputTypeRename="SPLIT_ORIGINAL" outputType="SPLIT_COPY"/>
    <filter class="[tokentype].TokenTypeProcessFilterFactory" includeTypes="SPLIT_COPY" _class="solr.ICUFoldingFilterFactory"/>
    <filter class="[tokentype].TokenTypeJoinFilterFactory" inputTypes="SPLIT_COPY,SPLIT_ORIGINAL" outputType="SPLIT_JOINED"/>
  </analyzer>
</fieldType>
```
Minor modifications to the solr core code allow all the logic of how
term values should be represented externally to be implemented at the
`FieldType` level -- a fully-supported Solr plugin extension point.

## JSON Reference Payload fields

`JsonReferencePayloadTokenizerFactory` will parse a string containing
a serialized JSON object describing references to other
terms. `JsonReferencePayloadHandler` is used to create the facet
payloads in Solr responses.

The use case for this is a facet field which you want to be able to
browse and display cross-references for.

You'll need to set up a fieldType definition in the schema.xml file:

```xml
<fieldType name="xfacet" class="edu.upenn.library.solrplugins.CaseInsensitiveSortingTextField" payloadHandler="edu.upenn.library.solrplugins.JsonReferencePayloadHandler" sortMissingLast="true" omitNorms="true">
  <analyzer>
    <tokenizer class="edu.upenn.library.solrplugins.JsonReferencePayloadTokenizerFactory"/>
  </analyzer>
</fieldType>
```

Fields can then be defined as follows:

```xml
<field name="subject_xfacet" type="xfacet" indexed="true" stored="true" multiValued="true" />
```

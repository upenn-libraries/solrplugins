# Solr Plugins to support normalized term browse with term-based cross-references

The focus of this project is to extend native Solr faceting functionality
to support a range of commonly-desired use cases. The main aspects of extension
complement each other, but are mostly orthogonal:
1. normalized index-order sorting (e.g., case-insensitive)
2. arbitrary index-order result windows, specified by target term, offset, and limit
   (in place of Solr's existing "prefix+offset" result window specification)
3. inclusion of per-term metadata in facet results (e.g., term cross-references)
4. support for specification of complex term mapping (e.g., synonym expansion)
   and term metadata generation, dynamically generated by the external indexing
   process and passed to Solr at index time.
5. support for document-centric display/expansion of facet term browsing

## 1. Normalized index-order sorting

The approach to normalized sorting of terms is accomplished by
having the value written to the index on disk be the raw input, prepended by a 
normalized (case-folded, etc.) version of the raw input (and optionally followed
by any nonfiling prefix such as "The " in the case of a title), with each part
delimited by a specified number of null bytes. 

The resulting sorting behavior could in theory be achieved external to Solr (with
a stock Solr implementation) by pre-processing fields in the indexing client and 
constructing Solr input/update docs in this way, and processing on client-side at 
query time to extract the raw (display) value from the 
normalized-sortable indexed value. This would not be ideal, for two 
key reasons:
1. Transfering content including null bytes has many potential pitfalls in practice
2. It fails to take advantage of Solr's built-in standardized, highly-customizable
normalization and token analysis capabilities.

The classes in the `tokentype` package avoid these drawbacks by taking advantage
of Lucene token types (as defined in the `org.apache.lucene.analysis.tokenattributes.TypeAttribute`
class) to "fork" tokens (assigning different token types to each fork), 
selectively process tokens according to type (delegating the actual
processing to standard solr/lucene analyzers), and merge/concatenate
adjacent tokens into null-delimited single output tokens.

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

## 2. Arbitrary index-order result windows

Stock Solr currently provides only one way to specify the result window for index-sorted
facets: prefix + offset. This is a fairly blunt instrument for many use cases, and for 
fields with a large number of values (hence requiring relatively large offsets), the
bluntness is more keenly felt, and this approach is actually somewhat less efficient than
an approach supporting more direct specification of target terms.

The target + offset + limit functionality introduced in this project is actually quite
similar to the prefix + offset functionality, although the nuances of implementation differ,
and the API semantics vary slightly: 

### API parameters

#### `facet.target`

Specifies a term whose ceiling (within the index, after processing by the fieldType's
queryAnalyzer) defines a reference point for the requested window of terms.

#### `facet.limit`

Specifies the requested size of the window of terms to be returned.

#### `facet.offset`

Specifies the requested offset of the target term (ceiling) with respect to the window
of terms returned. Semantics are analogous to zero-based array indexing.

#### Simple examples

Given an index of terms: `C D E F G`

The following request configurations would return the
following windows:

| target | limit | offset | returned window |
| ------ | ----- | ------ | --------------- |
| D | 1 | 0 | `C [D] E F G` |
| D | 1 | -1 | `C D [E] F G` |
| D | 1 | 1 | `[C] D E F G` |
| Da | 1 | 0 | `C D [E] F G` |
| Da | 2 | 0 | `C D [E F] G` |
| Fa | 2 | 2 | `C D [E F] G` |

#### "Edge" cases; complex examples

By design, the client has no a priori knowledge of the contents of the index,
and is issuing arbitrary stateless requests against a dynamic index. Because of this, it is
possible that the user may request a target/offset/limit combination that is impossible
to satisfy exactly as requested. To accommodate these cases, every target/offset response
of terms for a given field contains leading metadata specifying two values: `count`, and
`target_offset`. `count` is the number of terms actually included in the response (which
may be <= the requested `target.limit`). `target_offset` is the *actual* offset of the 
requested target term, with respect to the array of terms *actually* returned in the
response. (For the above "simple" examples, these response metadata values are omitted; 
in all cases `count` == `target.limit`, and `target_offset` == `target.offset`). Complex 
examples and discussion follow:

| target | limit | offset | returned window | count | target_offset |
| ------ | ----- | ------ | --------------- | ----- | ------------- |
| D | 2 | 2 | `[C D] E F G` | 2 | 1 |
| C | 2 | 2 | `[C D] E F G` | 2 | 0 |
| F | 2 | -1 | `C D E [F G]` | 2 | 0 |
| H | 2 | -1 | `C D E [F G]` | 2 | 2 |
| C | 6 | 0 | `[C D E F G]` | 5 | 0 |
| D | 6 | 0 | `[C D E F G]` | 5 | 1 |
| D | 6 | -1 | `[C D E F G]` | 5 | 1 |
| D | 6 | 6 | `[C D E F G]` | 5 | 1 |

*This behavior may be simply defined as giving priority to the requested `facet.limit`
over the requested `facet.offset`.*

In the context of a normal paging UI, this decision may initially seem strange; but
in fact it greatly simplifies the definition and implementation of the behavior, and
reduces the number of API calls and guesswork necessary from the point of view of the
client. By way of illustration, consider the following example: 

Over our 5-term sample index, a user enters an arbitrary target of "H", offset 0. Solr
must read the index, have the information at its fingertips, and respond to the user
in a way that indicates that the term information cannot be returned exactly as requested.
As an API, it is most useful for Solr to respond in a way that sends as much
possibly-relevant information as possible; it is up to the client application to
determine how, or whether, to expose that information to the end user.

## 3. Inclusion of per-term metadata in facet results

In stock Solr, little significance is attributed to the term *per se*, other than as a
key for document retrieval and search relevance ranking. What little data is associated
with an individual term-in-doc (position, offset, etc.) tends to be purely mechanical,
used for purposes of ranking and display context, etc. In many cases this is completely
appropriate, but in some cases it is useful to give clients more direct awareness of the
*context* of the inclusion of the term in the index. This can in fact be achieved by 
leveraging existing Solr infrastructure.

The motivating use case is one in which terms are highly normalized according to a linked
controlled vocabulary: subject and name authority headings. It is desired that users be
able to browse name headings, and see explicit cross-references from inline headings to
other related terms contained in the index. For instance, a user browsing the index for
"Clemens, Samuel Langhorne" should find an entry for "Clemens, Samuel Langhorne, 1835-1910",
with an associated count of works containing that heading exactly, but *also* a reference
(and associated count) to works containing the related heading, "Twain, Mark, 1835-1910". 

Behavior is similar to that of the standard Solr synonym filter, but transparently exposing
to the user the term from which the "synonym" was derived, and exposing some extra information
about the *relationship* between the two terms (cf. the simple "equivalence" relationship
implied by the existing Solr synonym filter).

### Implementation

The implementation of this functionality is achieved by leveraging the Solr
`PayloadAttribute` to record term relationships.

The Solr `PayloadAttribute` is a per-term-per-doc attribute (similar to 
`PositionIncrementAttribute` and `OffsetAttribute`) used by Solr to associate
arbitrary binary payloads with individual instances of terms in documents. This is
currently used mainly to support relevance boosting of particular instances
of terms (see the `DelimitedPayloadTokenFilterFactory` and `PayloadQParserPlugin`).
Even if one wanted to use `PayloadAttribute`s for this purpose, boosting is not
relevant in a faceting context. So we will repurpose the `PayloadAttribute` in a
way that *is* relevant in a faceting context: to encode term relationship metadata!
Relationships are recorded per-term-per-doc, and parsed and aggregated at query time
for inclusion as term metadata in the "extended" facet response.

## 4. Support externally specified complex term expansion

As mentioned above, the per-term "reference" metadata is quite similar to the
static synonym expansion provided natively in Solr via the `SynonymFilter`; but the 
volume and complexity of the vocabulary and references can quickly make a static
approach unsustainable. This was in fact the case with the name and subject authorities
of our motivating use case.

The need to support dynamic reference/synonym generation is clear, but to implement it
as a Solr TokenFilter in the server-side analyzer would require deploying heavyweight
reference resolution tools on all Solr servers in a potential SolrCloud deployment, and
would require *running* such tools (with attendant resource and performance implications)
in a production environment that supports indexing *and* end-user querying. 

The solution was to offload the dynamic reference/synonym generation to an external
indexing client. All generated references are passed to Solr as an JSON-encoded field
value, and a corresponding Solr `Tokenizer` configured on the Solr server parses the
JSON-encoded field and emits tokens accordingly, as if the references had been simply
generated as part of a standard server-side Solr analysis chain.

### JSON Reference Payload fields -- implementation overview and usage

`JsonReferencePayloadTokenizerFactory` will parse a string at index or query time
containing a serialized JSON object describing a main term, and may at index-time also
contain references to other terms. `JsonReferencePayloadHandler` is used at query-time
to read term references out the `PayloadAttribute` of term/doc postings, and build
the term metadata (aggregated references) for Solr query responses.

The motivating use case for this is a facet field which you want to be able to
browse and display cross-references for.

You'll need to set up a fieldType definition in the schema.xml file:

```xml
<fieldType name="xfacet" class="edu.upenn.library.solrplugins.CaseInsensitiveSortingTextField" payloadHandler="edu.upenn.library.solrplugins.JsonReferencePayloadHandler" sortMissingLast="true" omitNorms="true">
  <analyzer type="index">
    <tokenizer class="edu.upenn.library.solrplugins.JsonReferencePayloadTokenizerFactory"/>
    <!-- use SplitFilter to create 'normalized' token based on 'filing' token -->
    <filter class="edu.upenn.library.solrplugins.tokentype.TokenTypeSplitFilterFactory" includeTypes="filing" outputType="normalized" _class="org.apache.lucene.analysis.icu.ICUFoldingFilterFactory" />
    <filter class="edu.upenn.library.solrplugins.tokentype.TokenTypeJoinFilterFactory" inputTypes="normalized,filing,prefix" outputType="indexed" typeForPayload="normalized" outputComponentTypes="false"/>
  </analyzer>
  <analyzer type="query">
    <tokenizer class="edu.upenn.library.solrplugins.JsonReferencePayloadTokenizerFactory"/>
    <!-- use SplitFilter to create 'normalized' token based on 'filing' token -->
    <filter class="edu.upenn.library.solrplugins.tokentype.TokenTypeSplitFilterFactory" includeTypes="filing" outputType="normalized" _class="org.apache.lucene.analysis.icu.ICUFoldingFilterFactory" />
    <filter class="edu.upenn.library.solrplugins.tokentype.TokenTypeJoinFilterFactory" inputTypes="normalized,filing,prefix" outputType="indexed" typeForPayload="normalized" outputComponentTypes="true"/>
  </analyzer>
</fieldType>
```

Fields can then be defined as follows:

```xml
<field name="subject_xfacet" type="xfacet" indexed="true" stored="true" multiValued="true" />
```


## How is the project structured and updated?

The simplest and most sustainable approach to introducing this functionality involves
some minimal modification of a handful (4, as of 2016-09-16) of core Solr classes,
to provide hooks for extension via plugins. These modifications completely preserve
backward compatibility with stock Solr faceting behavior.

To simplify this patching of Solr classes, minimize the footprint of this project,
and enable deployment in standard environments (e.g., with the standard Solr docker
image), we opted not to fork Lucene-Solr and use a custom build. To allow the project
to keep pace with upstream Solr releases, a branch 'solr-upstream-base' is maintained
with stock versions of tagged releases for locally-modified Solr classes. A script
is maintained in that branch that specifies the solr files to be extended, and is
responsible for downloading them into the branch (to achieve a sort of
pseudo-remote-tracking branch). This branch may then be merged into master, integrating
upstream changes and making it apparent when any such changes require manual intervention.
Although this approach to extending Solr code may seem slightly unorthodox, it has
served very well thus far.



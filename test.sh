#!/usr/bin/env bash

BASE_DIR="$(cd "$(dirname "$0")" && pwd -L)"
SOLR_UPSTREAM_BASE="solr-upstream-base"

cd "$BASE_DIR"

current_branch="$(git rev-parse --abbrev-ref HEAD)"

if [ "$current_branch" != "$SOLR_UPSTREAM_BASE" ]; then
  echo "not on branch \"$SOLR_UPSTREAM_BASE\"" >&2
  exit 1
fi

latest_tag=$(git ls-remote --tags git@github.com:apache/lucene-solr.git 'refs/tags/releases/lucene-solr/*' | grep -v '\^' | sort -t '/' -k 3 -V | tail -n 1)

ref="${latest_tag##*refs/tags/}"

while read file; do
  mkdir -p "$BASE_DIR/src/main/java/${file%/*}"
  curl -s "https://raw.githubusercontent.com/apache/lucene-solr/$ref/solr/core/src/java/$file" > "$BASE_DIR/src/main/java/$file"
done << EOF
org/apache/solr/handler/component/FacetComponent.java
org/apache/solr/request/SimpleFacets.java
org/apache/solr/request/DocValuesFacets.java
EOF

git add .
git commit -m "solr upstream base for tag \"$ref\""

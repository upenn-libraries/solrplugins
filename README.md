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

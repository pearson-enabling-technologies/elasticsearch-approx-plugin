Release 2.1.6
-------------
Added post/collector mode support for termlist plugin 


Release 2.1.5
-------------

More documentation improvements, code cleanup.

Workaround for a compiler issue in some versions of Java 6:

https://github.com/pearson-enabling-technologies/elasticsearch-approx-plugin/issues/41

Release 2.1.4
-------------

Documentation improvements.

Release 2.1.3
-------------

Fixed a race condition in BytesRefUtils.

Release 2.1.3
-------------

Better checking for incorrect usage of distinct count payloads, and terminology improvements.

Added "sample" parameter for term list facet's equivalent of approximate mode.

Moved term list facet over to use BytesRefHash storage, like date facet.

Release 2.1.2
-------------

Improvements to speed and memory management in exact distinct facets,
by using Lucene's BytesRefHash instead of Trove hashmaps to store field values.

Release 2.1.1
-------------

Started keeping changelog (our bad).

Partial refactoring of Collectors for date facets.

Minor improvements to memory usage.


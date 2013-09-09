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


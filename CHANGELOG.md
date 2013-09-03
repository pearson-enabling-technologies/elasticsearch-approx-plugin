Release 2.1.1
-------------

Started keeping changelog (our bad).

Partial refactoring of Collectors for date facets.

Minor improvements to memory usage.

Release 2.1.2
-------------

Improvements to speed and memory management in exact distinct facets, by using Lucene's BytesRefHash instead of Trove hashmaps to store field values.

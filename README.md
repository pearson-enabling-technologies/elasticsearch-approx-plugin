ElasticSearch Approx Plugin
===========================

Plugin to use approximate methods for enabling and/or speeding up certain queries.

Currently just provides one such query: distinct date histogram.

Distinct Date Histogram
-----------------------

Counts the total numbers of terms and _unique_ terms in a given field, for each
interval in a date range.

For example:

    curl -XPOST "http://localhost:9200/pop1/_search?search_type=count&pretty=true" -d'{
        "query": {
            "match_all" : {}
        },
            "facets" : {
                "histo" : {
                    "distinct_date_histogram" : {
                        "key_field" : "datetime",
                        "value_field" : "userid",
                        "max_exact_per_shard" : 100,
                        "interval" : "hour"
                    }
                }
            }
    }'

Returns something like:

    {
      "took" : 3966,
      "timed_out" : false,
      "_shards" : {
        "total" : 5,
        "successful" : 5,
        "failed" : 0
      },
      "hits" : {
        "total" : 24131101,
        "max_score" : 0.0,
        "hits" : [ ]
      },
      "facets" : {
        "histo" : {
          "_type" : "distinct_date_histogram",
          "entries" : [ {
            "time" : 1333580400000,
            "count" : 2133,
            "distinct_count" : 1515
          }, {
            "time" : 1333584000000,
            "count" : 191991,
            "distinct_count" : 12407
          }, {
              ...
          }, {
            "time" : 1333753200000,
            "count" : 238022,
            "distinct_count" : 26092
          } ]
        }
      }
    }

The facet works initially by keeping a hashset of all terms encountered, but
when this grows too large, falls back to an approximate counting method using
Adaptive Counting from stream-lib:

https://github.com/clearspring/stream-lib

The `max_exact_per_shard` parameter controls how many distinct values will be
gathered from a single shard, before migrating to the approximate count
instead. This can be used to keep memory usage under control. It defaults to
1000.


Installing
----------

Package into a zip (in `target/releases`):

    mvn package

Then create a `plugins/approx` directory in your ElasticSearch installation,
and unzip the release zipfile into there.


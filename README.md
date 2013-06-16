# ElasticSearch Approx Plugin

A plugin for [ElasticSearch](http://www.elasticsearch.org/) to use approximate
methods for certain queries, to greatly reduce memory usage and network traffic.

Approximate counting is performed via the excellent probabilistic data
structures from [stream-lib](https://github.com/clearspring/stream-lib).

It currently just provides one such query: distinct date histogram.

This work is inspired in part by
[elasticsearch-ls-plugins](https://github.com/lovelysystems/elasticsearch-ls-plugins)
by Lovely Systems -- some of their code has been reused here.

## Distinct Date Histogram

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
when this grows too large, it falls back to an approximate counting method.

The `max_exact_per_shard` parameter controls how many distinct values will be
gathered for a given bucket from a single shard, before migrating to the
approximate count instead. This can be used to keep memory usage under control.
It defaults to 1000.

The approximate counting method is hard-coded to HyperLogLog method with a
relative standard deviation of 0.0025, which uses about 80KB of memory per
bucket per shard.

An obvious extension would be to give users the option to control the algorithm
used, and its parameters, so they can tune these based on the expected
cardinality of the field in question, and the desired memory usage. (Pull
requests gratefully received.)

Note that the counts are based on terms, so if the `value_field` is tokenized,
the result won't indicate the number of distinct _values_ in that field, but
rather, the number of distinct tokens (post-analysis).

## Building and testing

There's a pre-built distro, see below. But if you want to play, read on.

It's all done via Maven, so just `mvn test` to build the plugin and run the
tests. Amongst other things, they check that the distinct counts are within a
tolerance of 1% of the expected values.

If you get errors like `OutOfMemoryError[Direct buffer memory]]]` or other
weird errors from ElasticSearch, then you may need to raise the amount of
memory you allocate to the mvn process. Try using `-Xmx1G` in the `MAVEN_OPTS`
variable. (If you're an Eclipse user, put `-Xmx1G` in the VM Arguments box of
the Arguments tab in Run Configurations for that test.)

Yes, this is quite a lot of memory for unit tests, but the tests use several
iterations of randomly generated data of increasing size, in order to verify
the accuracy of the approximate counts. The final run puts over a million
distinct values in each bucket. For the same reason, the tests take several
minutes to run.

### Important note

Because the error rate for HyperLogLog is a distribution rather than a hard
bound, you may occasionally get tests failed due to results being just outside
the 1% tolerance. If this happens, re-run the test. It's only a problem if it
happens consistently...

## Installing

From your ElasticSearch root directory, type:

    bin/plugin -install ptdavteam/elasticsearch-approx-plugin/<VERSION>

e.g.

    bin/plugin -install ptdavteam/elasticsearch-approx-plugin/1.0.8

If you're building from source, just do

    mvn package

to rebuild the zipfile (in `target/releases`). You may want to add
`-DskipTests=true` if you've just run the tests and haven't changed anything,
since they take so long.

## Credits

This project was developed by the Data Analytics & Visualization team
at Pearson Technology in London.

http://www.pearson.com/

## License

    Copyright 2012 Pearson PLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.


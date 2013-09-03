# ElasticSearch Approx Plugin

A plugin for [ElasticSearch](http://www.elasticsearch.org/) to use approximate
methods for certain queries, to greatly reduce memory usage and network traffic.

Approximate counting is performed via the excellent probabilistic data
structures from [stream-lib](https://github.com/clearspring/stream-lib).

This work is inspired in part by
[elasticsearch-ls-plugins](https://github.com/lovelysystems/elasticsearch-ls-plugins)
by Lovely Systems -- some of their code has been reused here.


## Version compatibility

Plugin < 1.3.0: ElasticSearch 0.19.X, tested on 0.19.11

Plugin 1.3.X: ElasticSearch 0.20.X, tested on 0.20.6

*Plugin 2.0.1: Interim release, don't use this, try the next one...*

Plugin 2.1.1: ElasticSearch 0.90.2, plus significant feature and performance improvements, and breaking API changes

ElasticSearch 0.90.3 is not supported yet.

**N.B.** If you are upgrading from a previous version to 2.1.0, please read the
following carefully, as the syntax (and semantics) have changed in several places.


## Date Facet

This is an alternative to ElasticSearch's date histogram facet which can be used in
several modes.

* Counting records per time interval (like the built-in date histogram)

* Counting occurrences of a field, per time interval

* Counting unique values of a field, per time interval

* Counting records per time interval, per value of another field

* Counting occurrences of one field per time interval, per value of another field

* **COMING SOON:** Counting unique values of one field per time interval, per value of another field

It can be used to answer analytical queries like "how many distinct users have
I seen per day?" or "how many logins have occurred per day, broken down by
country?".

Unique value counting uses a [probabilistic algorithm](http://www.infoq.com/presentations/scalability-data-mining)
called [HyperLogLog](http://metamarkets.com/2012/fast-cheap-and-98-right-cardinality-estimation-for-big-data/)
to provide estimates of the number of distinct values without needing to store
all values in memory or transfer them across the network between shards. This
provideds both memory and speed improvements in most circumstances. This
implementation is hardcoded to a relative standard deviation of 0.0025, which
uses about 80KB of memory per bucket per shard, and in tests, provides
estimates within 1% of the true count reliably. 

The API for approximate counting also provides an `exact_threshold` parameter.
Each bucket will use an exact counting method (keeping all values in a HashSet)
until this point is reached. Then it will fall back to using HyperLogLog. If
you set this value to -1 it will never use approximate counting -- don't do
this on very large data sets as you will probably get out-of-memory errors. If
you set it to 0, it will never store any values in sets, instead using
HyperLogLog from the start.

### Syntax

```javascript
{
    "query" : {
        "match_all" : {}
    },
    "facets" : {
        "histo1" : {
            "date_facet" : {
                "key_field" : "my_date_time",
                "distinct_field" : "user_id",
                "interval" : "day"
            }
        }
    }
}
```

This example will count the number of distinct user IDs per day.

Parameters:

* `key_field`: The datetime field to facet on

* `value_field`: A field to count occurrences of

* `distinct_field`: A field to count distinct occurrences of

**N.B.** You can't use `value_field` and `distinct_field` at the same time.

* `slice_field`: A field to further subdivide the results by

* `exact_threshold`: See above

* `interval`, `time_zone`, `pre_zone`, `post_zone`, `pre_zone_adjust_large_interval`, `pre_offset`, `post_offset`, `factor`: See docs for the [date histogram facet](http://www.elasticsearch.org/guide/reference/api/search/facets/date-histogram-facet/).

Of these, only `key_field` and `interval` are required -- this will perform the
simplest kind of record count.

All of the field parameters support tokenized/multi-valued fields as well as
single-valued ones. (The usual caveats about memory use apply.) e.g. If
`distinct_field` is tokenized, the result will indicate the number of distinct
tokens found in that field (post-analysis).

### Output

This is very similar to the standard date histogram. Each time period (and the
facet overall) has a `COUNT` attribute, and if appropriate, a `DISTINCT_COUNT`
attribute too. If you are using `slice_field`, these are provided for each time
period and for each slice within that time period.

### Limitations

* Using `slice_field` and `distinct_field` together is not yet tested

* Floating-point fields are not officially supported (they may work but we haven't really tested them thoroughly enough)

* Script fields are not yet supported


## Term list facet

This is a simple facet to quickly retrieve an unsorted term list for a field,
if you don't care about counts or ordering etc. It allows you to set a
`max_per_shard` cutoff similar to the previous facet (defaults to 100).

```javascript
{
    "query": {
        "match_all" : {}
    },
    "facets" : {
        "term_list_facet" : {
            "term_list" : {
                "key_field" : "txt1",
                "max_per_shard" : 100
            }
        }
    }
}
```

Returns something like:

```javascript
{
  "took" : 45,
  "timed_out" : false,
  "_shards" : {
    "total" : 3,
    "successful" : 3,
    "failed" : 0
  },
  "hits" : {
    "total" : 132,
    "max_score" : 1.0,
    "hits" : [ ]
  },
  "facets" : {
    "term_list_facet" : {
      "_type" : "term_list",
      "entries" : [ "trdq", "amrqkke", "ebztmm", "pja", "qobmepbor", "bxpoh", "krsm", "kpgz", "hotodwfq", "qbpxxlfin", "lsnosgx", "qyznyhrqcu", "poekzt", "qbsmks", "adbazy", "swnjdvziqh", "eqabkxb", "xdz", "jlg", "scn", "jdn" ]
    }
  }
}
```

This facet doesn't actually use anything clever like approximate counting --
it's not really approximate in the same sense as the previous one -- but we
thought you might find it useful.

**N.B.** The `use_field_data`/`read_from_cache` option from previous versions
is no longer supported. The plugin now uses ElasticSearch's field data cache
exclusively.


## Building and testing

It's all done via Maven, so just `mvn test` to build the plugin and run the
tests. Amongst other things, they check that the approximate distinct counts
are within a tolerance of 1% of the expected values.

To target Java 7 in the compiler (assuming you're running under JDK7), append
`-Pjdk7` to the `mvn` command line, e.g.:

`mvn clean test -Pjdk7`

To run the full test suite, you will need to download a fairly large archive
of ElasticSearch index data from here:

https://pearson.app.box.com/s/uvsz0gv8rhgex0aacc2u

Download `MediumDataSetTest.tar.bz2` and unpack it in `src/test/resources/data`.

The tests use quite a lot of memory and take several minutes to run. This is
because they use several iterations of randomly generated data and queries, in
order to verify the accuracy of the approximate counts in the date histogram,
among other things. One run puts over a million distinct values in each bucket.

If you get any out-of-memory errors, you'll need to raise the amount of memory
you allocate to the mvn process. From the command line, the pom takes care of
this via the argLine parameter. If you're an Eclipse user, put ` -Xms4G -Xmx4G`
in the VM Arguments box of the Arguments tab in Run Configurations for that
test run.

You can always build the package with `-DskipTests` if this is a problem
(assuming you trust us to have tested before checking in).

### Important note

Because the error rate for HyperLogLog is a distribution rather than a hard
bound, you may in rare circumstances get tests failing due to results being
just outside the 1% tolerance. If this happens, re-run the test. It's only a
problem if it happens consistently...


## Installing

**NB Github no longer supports file downloads. For now, you'll have to build
from source. We will find a location for binary distributions as soon as we can...
Promise :-)**

Type

    mvn package

to rebuild the zipfile (in `target/releases`). You may want to add
`-DskipTests` if you've just run the tests and haven't changed anything,
since they take a while.

Then create a `plugins/approx` directory in your ElasticSearch install dir,
and unzip the zipfile into there.


## Credits

This project was developed by the Data Analytics & Visualization team
at Pearson Technology in London.

http://www.pearson.com/


## License

    Copyright 2012-2013 Pearson PLC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.


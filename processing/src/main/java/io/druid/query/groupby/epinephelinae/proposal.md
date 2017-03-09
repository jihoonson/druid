# Motivation

Join is a very popular and useful query type for OLAP. Druid currently doesn't support join due to [performance and complexity issues](http://druid.io/docs/latest/querying/joins.html). However, one of very popular join patterns in OLAP is a _star join_, i.e., joining a large fact table with many small dimension tables. Supporting this kind of workload with reasonable performance will be greatly useful for druid users. 

# Proposed Join implementation

In this proposal, I'm focusing on the star join. In addition, inner equi join is only considered here.

## Join query 

Unlike Union, Join is regarded as a query type rather than a data source. Since a query can be used as a data source in Druid, join can be used as both a general query type and a data source.

A join query contains a ```JoinSpec``` which represents a join relationship between two sources. It can have another ```JoinSpec``` as its child to represent joins of multiple data sources. Unlike other query types, the join query's data sources are specified in ```JoinSpec```. The ```predicate``` in a ```JoinSpec``` can include logical operators and arithmetic operators. ```dimensions``` and ```metrics``` specify output column names.  

Here is an example.

```json
{
  "queryType": "join",
  "joinSpec" : {
    "type" : "INNER",
    "predicate" : {
      "type" : "equal",
      "left" : {
        "dataSource" : "foo",
        "dimension" : { "type" : "default", "dimension" : "dim1" }
      },
      "right" : {
        "dataSource" : "bar",
        "dimension" : { "type" : "default", "dimension" : "dim1" }
      }
    },
    "left" : {
      "type" : "dataSource",
      "dataSource" : "bar"
    },
    "right" : {
      "type" : "joinSource",
      "joinSpec" : {
        "type" : "INNER",
        "predicate" : {
          "type" : "and",
          "left" : {
            "predicate" : {
              "type" : "equal",
              "left" : {
                "dataSource" : "foo",
                "dimension" : { "type" : "default", "dimension" : "dim2" }
              },
              "right" : {
                "dataSource" : "baz",
                "dimension" : { "type" : "default", "dimension" : "dim1" }
              }
            }
          },
          "right" : {
            "predicate" : {
              "type" : "equal",
              "left" : {
                "dataSource" : "baz",
                "dimension" : { "type" : "default", "dimension" : "dim2" }
              },
              "right" : {
                "predicate" : {
                  "type" : "add",
                  "left" : {
                    "dataSource" : "foo",
                    "dimension" : { "type" : "default", "dimension" : "dim3" }
                  },
                  "right" : {
                    "dataSource" : "foo",
                    "dimension" : { "type" : "default", "dimension" : "dim4" }
                  }
                }
              }
            }
          }
        },
        "left" : {
          "type" : "dataSource",
          "dataSource" : "foo"
        },
        "right" : {
          "type" : "dataSource",
          "dataSource" : "baz"
        }
      }
    }
  },
  "dimensions" : [ "foo.dim1", "foo.dim2", "bar.dim5", "baz.dim5"],
  "metrics" : [ "foo.met1", "bar.met2" ],
  "filter": { "type": "selector", "dimension": "baz.dim1", "value": "some" },
  "granularity" : ...,
  "intervals" : ...,
  "virtualColumns" : ...,
  "context" : ...
}
```

This query can be expressed in sql like
```sql
SELECT
  foo.dim1, foo.dim2, bar.dim5, baz.dim5, foo.met1, bar.met2
FROM
  bar, foo, baz
WHERE
  foo.dim1 = bar.dim1 AND 
  foo.dim2 = baz.dim1 AND
  baz.dim2 = foo.dim3 + foo.dim4
```

or 
```sql
SELECT 
  foo.dim1, foo.dim2, bar.dim5, baz.dim5, foo.met1, bar.met2
FROM
  bar INNER JOIN foo ON foo.dim1 = bar.dim1
  INNER JOIN baz ON foo.dim2 = baz.dim1 AND baz.dim2 = foo.dim3 + foo.dim4
```
.

## Design

Among many well-known distributed join algorithms, _broadcast join_ is known as an efficient algorithm when joining a small table with a large table. 
In broadcast join, the small table is replicated to all processing nodes. Thus, those processing nodes can perform join without further data exchange.

### Broadcasting data

In Druid, data can be broadcasted at data ingestion time using [_load rules_](http://druid.io/docs/latest/operations/rule-configuration.html). To do so, the following changes need to be made.

- Add ```broadcast``` or ```full replication``` as a new replication factor
- Once this option is set for a data source, all segments of the data source are replicated to all historicals and _realtimes_.
  - Whenever new servers are added, all segments of broadcasted data sources are automatically stored on those new servers.
- Broadcasted segments are specially treated. It means, brokers, historicals, and realtimes can figure out that a given segment is a broadcasted one or not. This is because they need to know which segments are broadcasted for query planning. Please refer to the next section for details. 

### Join query processing

A join query is processed as follows.

- When a join query is submitted, the broker checks that data sources to be read are broadcasted or not
  - If all data sources are broadcasted, join must be performed in a single node.
  - If all data sources are broadcasted except one, join can be performed in multiple nodes holding the segments of the non-broadcasted data source.
  - Otherwise, join cannot be performed.
- Then, historicals and realtimes join broadcasted data sources and segments of the non-broadcasted data source.
  - Each node first performs the join of broadcasted data sources if there exist inner join relationships among them. This is done only one time in a node.
  - The join result of broadcasted data sources is joined with non-broadcasted segments by multiple threads in parallel. 
- The broker collects and merges bySegment join results

#### Hybrid hash join
Hash join is a simple and efficient solution for equi join. When joining broadcasted data sources, the size of join result can be large even though inputs are small data sources. Thus, the hybrid hash join is desired.

#### Runtime join ordering
Join ordering is significant for efficient join processing. When historicals and realtimes join broadcasted segments, they first choose the cheapest join of two segments and join them. And then, again, they choose the cheapest one among joins of segments and the previous join result. They repeat this until all segments are joined.

## GroupBy after Join

One of very popular query patterns is _groupBy after join_. This can be expressed by specifying a join as a query data source of a groupBy in Druid.
Once this kind of query is submitted, data nodes (historicals and realtimes) first process join and then immediately perform the groupBy against the join result. Finally, brokers collect bySegment groupBy results. 

# Future plan

- Join result (or hash table) caching
- Other join types (like outer or anti joins) support
- Supporting non-equi joins
- Supporting join after groupBy
- Supporting join of non-broadcasted, but partitioned data sources
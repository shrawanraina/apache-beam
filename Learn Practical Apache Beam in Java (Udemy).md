# Learn Practical Apache Beam in Java (Udemy)

## Batch processing vs Real-time processing

### Batch

- For processing high volume data where a group of transactions are collected
  over a period of time
- Hadoop is the framework for batch processing

#### Advantages:

- Process large volumes of data
- Can be done during off-peak hours
- Cost effective

#### Disadvantages:

- Time delay between collection and result
- Master file is not always kept up to date

### Real-time

Involves continuous input, process and output of data

#### Advantages:

- No significant delay
- Information is up to date

#### Disadvantages:

- Expensive
- Auditing is difficult

### Apache Beam

1. Data Source: `Unified API Model as it can handle both batch and stream`
    - Batch
    - Stream
2. Cluster/Runner: `Portable`
    - Spark
    - Flink
    - Apache Apex
    - Google Data Flow
    - Samza (Code once and run in any cluster)
3. SDK
    - Java
    - Python
    - Go

### PCollection

Distributed data set that beam pipeline operates on

- Element type should be same
- Immutability (object cannot be modified)
- Random access is not allowed
- PCollection can be either bounded or bounded
- Element has a timestamp

#### Ability to read from local file TEXTIO

#### Ability to read from a Java object (e.g. List) - using MapElements and TypeDescriptors

#### Can extend PipelineOptions class and use command line arguments

### PTransform

- Data processing operation, or a step, in the pipeline

### Map

- Element wise transformation
- Can convert PCollection object from one form to another

#### Map variations

- MapTuple: Manipulates tuple-based collections.
- MapElements: Transforms each element using a given function.
- MapKeys: Modifies keys in key-value pair collections.
- MapValues: Alters values in key-value pair collections.
- FlatMap: Maps input elements to zero or more output elements. Handy when
  breaking a text row at delimiters

### ParDo

- Element wise transform
- Invoking a user specified function on each of the elements (can produce zero
  or more output elements)

### Filter

- Should implement Serializable function for Filter transform

```
Filter.by() // applies the filtering logic on each input
```

### Flatten

- Combines multiple PCollection into single PCollection object
- Merges the PCollections from different sources into a single PCollection.

```
Flatten.pCollections()
```

### Partition

- Reverse of Flatten
- One PCollection broken into multiple PCollection object

### Side Inputs

- Can provide addition input to ParDo transform

#### PCollectionView

- Immutable view of a PCollection that can be accessed as a side input to a
  ParDo transform
- View.asSingleton(), View.asIterable(), and View.asMap()

### Distinct

- To remove duplicate records from PCollection object

### Count

- To count the number of elements in a PCollection
  ```Count.globally()```

### GroupByKey

- Designed for PCollections of key-value pairs
- Group all values associated with a particular key and produces a new
  PCollection where each entry comprises a unique key and an iterable of values
  corresponding to that key

### Combine

- Aggregative transformation
- Compresses elements of a PCollection into a single cumulative output value
- Pre-built combine functions: sum, min, max
- For example: ```Mean.globally(), Count.globally()```

### Partition

- Divides the elements of a PCollection according to the partition function into
  each resulting partition PCollection

### Inner Join (CoGroupByKey)

- Performs a relational join of two or more key/value PCollections that have the
  same key type
- TupleTag

### Left Join

Returns all records from the left table and matched records from the right table

### Right Join

Returns all records from right table and matched records from left table

### Window

- Process real-time data in micro batches. Divide the data into multiple windows
  based on time
- Each element should have a timestamp
- Fixed Window:
- Sliding Window:
- Session Window:
- Global Window:
-

### S3 Integration

`TODO:`

### Parquet

- Open source file format
- Stores nested data structures ina flat columnar format
- Advantages:
    - Organizing by column allows for better compression (as daa is more
      homogenous)
    - I/O is reduced as we can efficiently scan only a subset of the columns
- Parquet only supports GenericRecord to read and write to Parquet

### Integration

#### JDBC

#### MongoDB

- Document based

#### HDFS

`TODO:`

#### BEAM SQL

Query PCollection with SQL statement

## References

- [Learn Practical Apache Beam in Java | BigData framework](https://www.udemy.com/course/learn-practical-apache-beam-in-java/learn/lecture/18862198?start=195#overview)
- [Mastering Apache Beam: Essential Transformations in Java for Google Cloud Dataflow](https://medium.com/@mrayandutta/mastering-apache-beam-essential-transformations-in-java-for-google-cloud-dataflow-ac5e83a7f47e)

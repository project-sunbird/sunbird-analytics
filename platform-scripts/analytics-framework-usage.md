## Analytics Framework Usage Document

### Introduction

This document is intended to provide a detail information of - how to use the framework (within spark-shell) and various adapters, dispatchers and utilities built into the framework.

Before going into the document go through the framework design document located [here](https://github.com/ekstep/Common-Design/wiki/Analytics-Framework-Design)

### Nomenclature

**spark-home** - Spark home directory

**models** - Directory where the framework and models/data products are stored. Models are classes which contain the actual algorithm

**sc** - Short hand for spark context

***

### Core Package Structures

Following are the core analytics package structures that are needed to be imported to work with the framework.

```scala

/**
 * The following package is the core package for importing the framework code. This package includes the following:
 * 1. High level APIs - DataFetcher, DataFilter and OutputDispatcher
 * 2. Generic models used across the framework and algorithms - Event (for telemetry v1), TelemetryEventV2 (for telemetry v2), MeasuredEvent (for derived events), adapter models and config
 * 3. JobDriver - to execute a job when provided with a config and the data type
 */
import org.ekstep.analytics.framework._

/**
 * This package contains all the utility classes/functions such as:
 * 1. CommonUtil - All common utililities such as creating spark context, date utilities, event utilies, setting s3 and cassandra conf etc.
 * 2. JSONUtils - for json serialization and deserialization. The APIs are generalized to deserialized to any object
 * 3. S3Util - Utilities to operate on S3 data. Upload, fetch keys, query between a date range etc
 * 4. RestUtil - Generalized rest apis - get and post
 */
import org.ekstep.analytics.framework.util._

/**
 * This package contains all the adapters currently implemented in the framework.
 * 1. ContentAdapter - To fetch content data from the learning platform
 * 2. DomainAdapter - To fetch domain data from the learning platform
 * 3. ItemAdapter - To fetch item data from the learning platform
 * 4. UserAdapter - To fetch user profile information from the learner database (MySQL)
 */
import org.ekstep.analytics.framework.adapter._
```

Following are the package structures that are required to be imported to invoke existing algorithms/models


```scala

/**
 * This package contains all the models currently implemented. All the models are required to extend IBatchModel and implement the generic interface <code>def execute(events: RDD[T], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext) : RDD[String]</code>. Following are the models implemented
 * 1. LearnerSessionSummary - To compute the session summaries from raw telemetry of version v1
 * 2. LearnerSessionSummaryV2 - To compute the session summaries from raw telemetry of version v2
 * 3. LearnerActivitySummary - To compute the learner activity summary for the past one week
 * 4. LearnerProficiencySummary - To compute the learner proficiency per concept
 * 5. RecommendationEngine - To compute the learner concept relevance to be used in RE
 */
import org.ekstep.analytics.framework.model._

/**
 * This package contains all the database updaters currently implemented. Database updaters are also implemeted as models so should extend IBatchModel and implement the <code>execute()</code> function. Following are the updaters implemented
 * 1. UpdateLearnerActivity - To store the learner activity snapshot into learner db
 * 2. LearnerContentActivitySummary - To store the learner activity per content. Used for RE
 */
import org.ekstep.analytics.framework.updater._

/**
 * This package contains all the jobs - to either run the models or the updaters. Each DP will have a corresponding job associated with it.
 */
import org.ekstep.analytics.framework.job._
```

### Core Framework Concepts

#### Fetch Data/Input

Following are the supported fetch types in the framework abstracted by `DataFetcher`:

1. S3 - Fetch data from S3
2. Local - Fetch data from local file. Using the local file once can fetch data from hdfs too.

Following are the APIs available in the DataFetcher

```scala

// API to fetch data. Fetch the data as an RDD when an explicit type parameter is passed.
val rdd:RDD[T] = DataFetcher.fetchBatchData[T](search: Fetcher);
```

A `Fetcher` object should be passed to the DataFetcher along with the type the data should be serialized to. Following are the example structures of the `Fetcher` object.

```scala

// Example fetcher to fetch data from S3
val s3Fetcher = Fetcher("s3", Option(Query("<bucket>", "<prefix>", "<startDate>", "<endDate>", "<delta>")))

// S3 Fetcher JSON schema
{
	"type": "S3",
	"query": {
    	"bucket": "ekstep-telemetry",
     	"prefix": "telemetry.raw",
    	"startDate": "2015-09-01",
    	"endDate": "2015-09-03"
	}
}

// Example fetcher to fetch data from Local

val localFetcher = Fetcher("local", Option(Query(None, None, None, None, None, None, None, None, None, "/mnt/data/analytics/raw-telemetry-2016-01-01.log.gz")))

// Local Fetcher JSON schema
{
	"type": "local",
	"query": {
    	"file": "/mnt/data/analytics/raw-telemetry-2016-01-01.log.gz"
	}
}
```

#### Filter & Sort Data

Framework has inbuilt filters and sort utilities abtracted by the DataFilter object. Following are the APIs exposed by the DataFilter

```scala
/**
 * Filter and sort an RDD. This function does an 'and' sort on multiple filters
 */
def filterAndSort[T](events: RDD[T], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[T]

/**
 * Filter a RDD on multiple filters
 */
def filter[T](events: RDD[T], filters: Array[Filter]): RDD[T]

/**
 * Filter a RDD on single filter
 */
def filter[T](events: RDD[T], filter: Filter): RDD[T]

/**
 * Filter a buffer of events.
 */
def filter[T](events: Buffer[T], filter: Filter): Buffer[T]

/**
 * Sort an RDD. The current sort supported is just plain String sort.
 */
def sortBy[T](events: RDD[T], sort: Sort): RDD[T]
```

The `DataFilter` can do a nested filter too based on bean properties (silimar to json filter). 

Structure of the `Filter` and `Sort` objects are 

```scala
/*
 * Supported operators are:
 * 1. NE - Property not equals a value
 * 2. IN - Property in an array of values
 * 4. NIN - Property not in an array of values
 * 5. ISNULL - Property is null
 * 6. ISEMPTY - Property is empty
 * 7. ISNOTNULL - Property in not null
 * 8. ISNOTEMPTY - Property in not empty
 * 9. EQ - Property in equal to a value
 */
case class Filter(name: String, operator: String, value: Option[AnyRef] = None);

case class Sort(name: String, order: Option[String]);
```

Example Usage:

```scala
val rdd:RDD[Event] = DataFetcher.fetchBatchData[Event](Fetcher(...));

// Filter the rdd of events to only contain either OE_ASSESS or OE_INTERACT events only
val filterdRDD = DataFilter.filter[Event](rdd, Filter("eid", "IN", Option(Array("OE_ASSESS", "OE_INTERACT"))));
```

#### Output Data

As discussed in the design document framework has support for the following output dispatchers:

1. ConsoleDispatcher - Dispatch output to the console. Basically used for debugging.
2. FileDispatcher - Dispatch output to a file.
3. KafkaDispatcher - Dispatch output to a kakfa topic
4. S3Dispatcher - Dispatch output to S3. Not recommended option. Can be used only for debugging purposes
5. ScriptDispatcher - Dispatch output to an external script (can be R, Python, Shell etc). Can be used for specific purposes like converting json output to a csv output (or) generating a static html report using R

Each and every output dispatcher should extend the `IDispatcher` and implement the following function

```scala
def dispatch(events: Array[String], config: Map[String, AnyRef]) : Array[String];
```

Output dispatchers are abstracted by the OutputDispatcher object.

APIs and example usage of OutputDispatcher

```scala
// APIs

// Dispatch to multiple outputs
def dispatch(outputs: Option[Array[Dispatcher]], events: RDD[String])

// Dispatch to single output
def dispatch(dispatcher: Dispatcher, events: RDD[String])

// Example Usage:
val rdd: RDD[String] = ....;
// File dispatcher
OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "/mnt/data/analytics/test_ouput.log")), rdd);

// Dispatcher JSON schema

{
	"to": "", // file, s3, kafka, console, script
	"params": {
		// Dispatcher specific config parameters. Like topic and brokerList for kafka
	}
}

// Dispatcher Schema definition
case class Dispatcher(to: String, params: Map[String, AnyRef]);
```

#### Utility APIs

***

### Examples

#### Connect to spark-shell

#### Example scripts to read data

#### Example scripts to filter data

#### Example scripts to dispatch output

#### End to End sample script
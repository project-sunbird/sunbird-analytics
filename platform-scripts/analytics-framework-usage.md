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

#### Filter & Sort Data

#### Output Data

#### Utility APIs

***

### Examples

#### Connect to spark-shell

#### Example scripts to read data

#### Example scripts to filter data

#### Example scripts to dispatch output

#### End to End sample script
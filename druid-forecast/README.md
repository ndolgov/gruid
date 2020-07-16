#### 1. Overview

##### 1.1 Original inspiration 

This module originated in a job interview coding task a couple years ago. There was a need for applying 
linear regression-style forecasting logic to the results of a Druid query. The application was rather naive 
(synchronous calls, buffering the entire dataset in memory). 

Implementing the logic inside of Druid was deemed unreasonable because I had not experience with its 
internals. My impression from a cursory look at the codebase was that it was rather obscure and had 
some unusual abstraction in the query execution path.

The historic data is assumed to be retrieved by a Druid groupBy query. Such a query has dimensions to group by,
metrics to aggregate, and a time granularity such as daily/weekly. The forecasting logic has to 
be able to apply the same aggregation functionality to the forecast output data as was executed by Druid
on raw historic data. 

##### 1.2 Key ideas

My first reaction was to have something more stream-oriented, "think Presto instead of Spark". That led me to
the gRPC idea I implemented in the druid-grpc module later. Another intention was to try a more modern approach to 
composing operators than the Volcano model. The whole thing was an experiment in how far I could go
in those directions. 

So this module 
* given a Druid groupBy query, a historic period of time to base the forecast on, and the number of days to forecast (see DruidForecastServiceSpec)
* retrieves the query results from Druid over gRPC (see BlockingRowBatchIterator)
* pumps the results (see DruidForecastService) into a pipeline of quasi-relational operators (see Operators) 
* as a stub, prints the final output

The same pipeline (see ForecastService) is used for all requests. 
* read chunks of input data row by row
* group data by all dimensions
* calculate a forecast for each group,
* aggregate each forecast values on time with the same granularity as the original query        

#### 3.Implementation notes

##### 3.1 Operators

All operators are implemented in the push-style. Some of them make assumptions specific to the problem at hand to simplify the implementation.
Dimension values are dictionary-encoded throughout the pipeline for efficiency.   

##### 3.2 Wiring

Wiring together the columnar gRPC transport and mostly row-oriented push-style operators looks messy. It
is partially the result of having operator code that predates druid-grpc. 
* BlockingRowBatchIterator helps to present the input as an iterator
* DruidForecastService does unnatural things to (A) instantiate the operator pipeline when a data schema is received
and (B) process gRPC success/failure notifications

#### 4. Lessons

##### 4.1 Dictionary encoding of dimension values
The current implementation was a compromise. Architecturally, I would generate those dictionaries in a hypothetical 
component responsible for data ingestion into Druid. The immutable, versioned dictionaries would be propagated to the
frontend (or the backend tier closest to it) by some means. 

The way it works right now would require buffering the entire output to decode dimension values back into human-readable 
string. 
 
##### 4.2 gRPC chunk to row iterator adaptor
 
A more elegant approach is probably necessary. The tension here is between creating an immutable pipeline and receiving
some required metadata together with the first data chunk. On the other hand, having an explicit chunk queue helps 
decouple network IO from the pipeline in a reasonable way. 

##### 4.3 Relational operators

Writing relational operators could be fun. But truth be told, the forecast functionality belongs to the Druid engine.
I thought about doing it but was defeated by some unusual and not documented internal Druid abstractions.  
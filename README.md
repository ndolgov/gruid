#### gRPC Druid extension PoC

For a sophisticated engine dealing with voluminous data Druid lacks means of retrieving query results efficiently.
Judging from [a rejected issue](https://github.com/apache/incubator-druid/issues/3891) I thought it was a matter of policy. 
I wanted to see what it takes to actually implement it. 

When I had troubles with transitive dependency conflicts and asked politely :) the kind folks on Druid dev mailing list
shared another [similar extension](https://github.com/apache/incubator-druid/pull/6798). I borrowed their Guava version 
conflict workaround based on shading Guava classes and manually setting a classloader during initialization. The rest of
it is not exactly what I have in mind so this PoC still makes sense to me.

There are two libraries in this project:
* druid-grpc-rowbatch is a library with support for efficient row encoding using protobuf
* druid-grpc is an actual [Druid extension](http://druid.io/docs/latest/development/modules.html) that can be plugged into
Druid to provide a gRPC network endpoint and completely bypass JSON over HTTP

##### Key techniques

There's more than one way to use protobuf for data row representation. I guided my first iteration with a few ideas traditional in analytics query engines.

* columnar formats - try columnar layout for data structures first, fall back to row-oriented if unsuccessful 
* micro-batching - never send a single row over the wire to amortize serialization and latency costs
* dictionary encoding of dimension values - the actual strings matter in the UI only; storage, data transfer, and common 
operations such as comparison are much more efficient with integer types
* collections library with support for primitive numeric types - there are Java collection libraries that avoid the penalty of autoboxing  

##### Alternatives

* Avro over gRPC - too generic for a first iteration, might happen later 
* Arrow / Flight - very new, the RPC part is under-documented, not used by Druid core anyway (and so promises more transitive dependency whack-a-mole fun)
* Avatica - it seems to have a binary, protobuf-based transport but there's not enough documentation. The Calcite integration
is rather complicated and it will take time to grok.

##### Realistic usage example

Please see the druid-forecast module README file for a PoC application using this approach as a client. 

##### Running locally

Executing ```DruidGrpcQueryRunnerTest``` is the easiest way to see this extension working with little hassle.

Otherwise, build both modules locally with ```mvn clean install```. Then follow the instructions from the next section.

###### Druid extension configuration

Assuming official [Druid tutorial](http://druid.io/docs/latest/tutorials/index.html) setup in place:
* copy the locally built extension uber JAR file
* edit Druid configuration to enable and configure the extension 
* start up Druid with tutorial configuration
* run DruidClientTest and enjoy output of ```tail -f  $DRUID_HOME/var/sv/broker.log```

```
mkdir -p $DRUID_HOME/extensions/druid-grpc/
cp druid-grpc-0.16.1-incubating-SNAPSHOT.jar $DRUID_HOME/extensions/druid-grpc/

cd $DRUID_HOME
vi $DRUID_HOME/conf/druid/single-server/micro-quickstart/broker/runtime.properties

druid.extensions.loadList=["druid-grpc"]
druid.grpc.enable=true
druid.grpc.port=20001
druid.grpc.numHandlerThreads=8
druid.grpc.numServerThreads=4

./bin/start-micro-quickstart
```
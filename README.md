#### gRPC Druid extension PoC

For a sophisticated engine dealing with voluminous data Druid lacks means of retrieving query results efficiently.
It seems to be a [matter of policy](https://github.com/apache/incubator-druid/issues/3891). I wanted to see what
it takes to actually implement it. It is work in progress currently.

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
* Arrow / Flight - very new, the RPC part is under-documented, not used by Druid core anyway
* Avatica - it seems to have a binary, protobuf-based transport but there's not enough documentation. The Calcite integration
is rather complicated and it will take time to grok.

##### References

* [protobuf HTTP extension spec](https://cloud.google.com/service-management/reference/rpc/google.api#httprule)
* [protobuf HTTP extension](https://github.com/googleapis/googleapis/blob/master/google/api/http.proto)
* [Scala grpcgateway blog](https://www.beyondthelines.net/computing/grpc-rest-gateway-in-scala/)   


##### Running locally

```DruidGrpcQueryRunnerTest``` is as far as the integration with Druid goes right now. 

I need to sort out DI with Guice 
(especially property injection and eager initialization) and unusual Druid coding style rules.
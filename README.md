# HiveInterceptor
A Flume Interceptor which can use HiveSQL to intercept event.

## Examples

this is a example flume config:

```properties
# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source
a1.sources.r1.type = netcat
a1.sources.r1.bind = localhost
a1.sources.r1.port = 44444

# Describe the interceptor
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.HiveInterceptorBuilder
a1.sources.r1.interceptors.i1.sql = select c1,c2,c3,md5(c4) from event where c1 is not null and c3 = '女'
a1.sources.r1.interceptors.i1.input-column-num = 5
a1.sources.r1.interceptors.i1.input-column-delimiter = ,
a1.sources.r1.interceptors.i1.delimiter = |

# Describe the sink
a1.sinks.k1.type = logger

# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1
```

when you send data like this to localhost:44444,
```shell script
nc -lk 44444
1,李华,男,17,18888888888
2,马丽,女,16,19999999999
3,陆洋,男,17,16666666666
```

after intercepting, you will got:
```log
null
2|马丽|女|16|33fbaa69f0c1c931312a92d81bb3eeab
null
```

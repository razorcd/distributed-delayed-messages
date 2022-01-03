# distributed-delayed-messages

Distributed Delayed Messages Application to handle event publishing by delaying, repeating, etc events at a particular time to different topics. 

Branch to use Mongo as Custom State store: https://github.com/razorcd/distributed-scheduler/tree/CustomStateStore

```
input -> distributed-delayed-messages -.--> output1
                                       `--> output2
```

### TODO:
 - [x] add cloud events
 - [ ] add load tests
 - [x] add multiple topics
 - [ ] integrate different stores
 - [ ] implement all Ideas
 
### Ideas for events behavior:

##### When to start publishing: 
 - delay message by X minutes/s/ms..
 - delay message until fixed time

##### When to stop publishing:
 - stop after X minutes/s/ms..
 - stop at fixed time
 - stop after publishing X times 
 
##### How often to publish (in case of repeated republishing)
 - time interval

##### On new events with same ID
 - overwrite old event
 - ignore event if value is duplicate (define value field)
 - ignore event if number value deviation below X (define value field)
 - fluctuation reducer (publish only after fluctuation is stable or X minutes)
 
#### Other properties:
 - durable (not lost on restart)  
 - retry on publish failure (times/interval)
 - dlq on unparseable input
 - microbatching in case of high throughput
 - output topic
 - output k/v serializer?
 
 
### Input event example:

```json
{
   "specversion":"1.0",
   "id":"id1",
   "source":"/source",
   "type":"DistributedDelayedMessagesEvent",
   "datacontenttype":"application/json",
   "dataschema":null,
   "time":"2021-12-30T11:54:31.734551Z",
   "data":{
      "message":"message0",
      "metaData":{
         "startAt":"2021-05-15T21:02:31.333824Z",
         "outputTopic":"outputTopic1"
      }
   }
}
```  
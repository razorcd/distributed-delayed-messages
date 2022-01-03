# distributed-delayed-messages

Distributed Delayed Messages Application to handle event publishing by delaying events at a particular time to different topics. 
Application uses kafka-streams to distribute load over multiple instances.

Branch to use Mongo as Custom State store: https://github.com/razorcd/distributed-scheduler/tree/CustomStateStore

### System
```
inputTopic -> distributed-delayed-messages -.--> outputTopic1
                                            `--> outputTopic2
```

### Use

System reads events from `inputTopic` stores them until configured and then publishes the `message` to selected `outputTopic`.

### Features:

- event format and explanations described below
- publishing multiple events with same partitionKey will result in publishing only the last one
- if publishing `startAt` time is in past, it will be published immediately
- `message` must be String format. Can be json. System does not deserialise it and will publish the message payload directly to outputTopic.
- outputTopic message partitionKey will be same as input partitionKey.
- `outputTopic` must be manually configured before publishing allowed.
- `outputTopic` can be published only to one topic.
- input/output topics must be manually created.
- partition if the inputTopic represents the distribution capacity (for now)
- more feature will come soon, see Ideas section


Input event format:
key: `000`

value:
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
- `message` represents the message/event that will be published to output topic
- `metaData` contains the scheduled publishing time and  payload of publishing 

This input will publish around `2021-05-15T21:02:31.333824Z` in `outputTopic1`:

key: `000`

value: `message0`


### TODO:
 - [x] add cloud events
 - [ ] add load tests
 - [x] add multiple topics
 - [ ] integrate different stores
 - [ ] implement all Ideas
 
### Ideas for more features:

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
 
 

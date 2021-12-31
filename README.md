# distributed-scheduler

Distributed Scheduler Application to handle event publishing by delaying, repeating, etc events at a particular time to different topics. 

Branch to use Mongo as Custom State store: https://github.com/razorcd/distributed-scheduler/tree/CustomStateStore

```
input -> distributed-scheduler -.--> output1
                                `--> output2
```

###TODO:
 - [x] add cloud events
 - [ ] add load tests
 - [x] add multiple topics
 - [ ] integrate different stores
 - [ ] implement all Ideas
 
###Ideas for events behavior:

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
 
 
### Event:

```json
{
  "id":"5010e38c-ff1e-4274-9aaa-b27efd7e5c49",
  "source":"/myApp",
  "specversion":"1.0",
  "type":"eventTypeHere",
  "time":"2020-12-28T21:57:31.744",
  "dataschema":null,
  "datacontenttype":"application/json",
  "data": "{\"restaurantId\":\"1001\",\"notification\":\"Picking up order at 22:05\"}"
}
```  
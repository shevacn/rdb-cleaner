With rdb cleaner, you can clean your redis by pattern.

### Features
- Multiple tasks
- Limit redis node qps
- Limit big key deletion strategy
- Record deleted keys with log file


### Build & Run
```shell
cd cmd

go build -o rdb-cleaner 

./rdb-cleaner -c config.json
```

### Example configuration file
```json

{
  "nodes": [
    {
      "id": "user",
      "addr": "data-user-redis.domain:6379",
      "pwd": "abcdefg",
      "qps": 10000
    },
    {
      "id": "room",
      "addr": "data-room-redis.domain:6379",
      "pwd": "abcdefg",
      "qps": 10000
    }
  ],
  "tasks": [
    {
      "nodeId": "user",
      "pattern": "user:login_time:*",  //redis scan pattern, see more: https://redis.io/commands/scan
      "action": "del"  //delete without limit
    },
    {
      "nodeId": "user",
      "pattern": "user:info:*:*",
      "action": "del",
      "delLimitByNum": {
        "0": 14000000  //limit max value of first *
      }
    },
    {
      "nodeId": "room",
      "pattern": "room:info:*:*",
      "action": "del",
      "delLimitByNum": {
        "1": 12000000  //limit max value of second *
      }
    }
  ]
}
```

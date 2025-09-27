# README
pub-sub based simple message push system.

## programs

### pub.py
- pub-sub #admin publisher
- pub-sub #channel_name publisher

### sub_admin.py
- pub-sub #admin subscriber

### sub_chat.py
- pub-sub #channel_name subscriber

## pubsub channel

### #admin

used for chat group administration
- create/dele channel
- join/leave to/from channel

### #channel_name

used for chat messages for each channel

## Usage

### redis
start a redis-server
```
$ redis-server &
```

### server-program

```
$ python sub_admin.py
```
sub_chat.py is dynamically forked from sub_admin.py.

### client-program

```
$ python pub.py --create channel
$ python pub.py --remove channel
$ python pub.py --join channel user
$ python pub.py --leave channel user
$ python pub.py --speak channel message
```
## data model

db_name = 5 is used.

### channel names

- type  : set
- key   : channel
- value : {channel_name, channel_name, ...}

### chat users

- type  : set
- key   : user:channel_name
- value : {user_name, user_name, ...}

### process

- type  : hash
- key   : process
- value :  {channel_name : pid, channel_name:pid, ...}

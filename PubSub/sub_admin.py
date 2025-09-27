# vim: set fileencoding=utf8

import json
import os
import signal
import syslog

import redis

import sub_chat

client = redis.StrictRedis(host='localhost', port=6379, db=5)
pubsub = client.pubsub()
pubsub.subscribe('admin')

def subscribe_to_channel(channel):
    if client.sismember('channel',  channel):
        return

    pid = os.fork()
    if pid > 0:
        # parent process
        client.sadd('channel', channel)
        client.hset('process', channel, pid)
    elif pid == 0:
        # child process
        sub_chat.listen(channel)

def unsubscribe_to_channel(channel):
    client.srem('channel', channel)
    client.delete('user' + ':' + channel)

    pid = client.hget('process', channel)
    if pid.isdigit():
        syslog.syslog('kill subscribe process[%s]' % pid)
        os.kill(int(pid), signal.SIGTERM)

        os.waitpid(int(pid), 0)

def join_user(channel, user):
    key = 'user' + ':' + channel
    client.sadd(key, user)

def leave_user(channel, user):
    key = 'user' + ':' + channel
    client.srem(key, user)

def init_subscribe():
    for channel in client.smembers('channel'):
        subscribe_to_channel(channel)

def main():
    init_subscribe()

    for message in pubsub.listen():
        if message['type'] == 'message':
            data = message['data']
            data = json.loads(data)

            # create channel
            if data['type'] == 'create':
                subscribe_to_channel(data['channel'])
            # remove channel
            elif data['type'] == 'remove':
                unsubscribe_to_channel(data['channel'])
            # join channel
            elif data['type'] == 'join':
                join_user(data['channel'], data['user'])
            # leave channel
            elif data['type'] == 'leave':
                leave_user(data['channel'], data['user'])
        else:
            print 'admin', message

if __name__ == '__main__':
    main()

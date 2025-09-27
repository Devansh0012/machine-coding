# vim: set fileencoding=utf8

import os
import syslog

import redis

def async_push_notify(channel, user, message):
    syslog.syslog('push message [%s] of channel [#%s] to user [%s]' %
                  (message, channel, user))

def listen(channel):
    client = redis.StrictRedis(host='localhost', port=6379, db=5)
    pubsub = client.pubsub()
    pubsub.subscribe(channel)
    syslog.syslog('[%s]subscribe to %s' % (os.getpid(), channel))

    for message in pubsub.listen():
        if message['type'] == 'message':
            syslog.syslog('received a message of channel [#%s]' % channel)
            key = 'user' + ':' + channel
            for user in client.smembers(key):
                async_push_notify(channel, user, message['data'])
        else:
            print 'admin', message

def main():
    pass

if __name__ == '__main__':
    main()

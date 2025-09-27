# vim: set fileencoding=utf8
import argparse
import json

import redis

client = redis.StrictRedis(host='localhost', port=6379, db=5)

def cmd_create_chat(channel):
    client.publish('admin',
              json.dumps({'type'    : 'create',
                          'channel' : channel}))

def cmd_remove_chat(channel):
    client.publish('admin',
              json.dumps({'type'    : 'remove',
                          'channel' : channel}))

def cmd_join_chat(channel, user):
    client.publish('admin',
              json.dumps({'type'    : 'join',
                          'channel' : channel,
                          'user'    : user}))

def cmd_leave_chat(channel, user):
    client.publish('admin',
              json.dumps({'type'    : 'leave',
                          'channel' : channel,
                          'user'    : user}))

def cmd_speak_chat(channel, message):
    client.publish(channel, message)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--create', nargs=1, metavar=('channel', ))
    parser.add_argument('--remove', nargs=1, metavar=('channel', ))
    parser.add_argument('--join',   nargs=2, metavar=('channel', 'user'))
    parser.add_argument('--leave',  nargs=2, metavar=('channel', 'user'))
    parser.add_argument('--speak',  nargs=2, metavar=('channel', 'message'))
    args = parser.parse_args()

    if args.create is not None:
        cmd_create_chat(*args.create)
    elif args.remove is not None:
        cmd_remove_chat(*args.remove)
    elif args.join is not None:
        cmd_join_chat(*args.join)
    elif args.leave is not None:
        cmd_leave_chat(*args.leave)
    elif args.speak is not None:
        cmd_speak_chat(*args.speak)

if __name__ == '__main__':
    main()

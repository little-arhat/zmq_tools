#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import zmq

def zmq_pusher(args):
    ctx = zmq.Context()

    bind_to = args[0]

    pub = ctx.socket(zmq.PUB)
    pub.bind(bind_to)

    while True:
        pub.send(sys.stdin.readline().strip())

if __name__ == '__main__':
    if not sys.argv[1:]:
        print('usage: %s bind_to ...' % sys.argv[0].split('/')[-1])
        sys.exit(1)

    try:
        zmq_pusher(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        pass

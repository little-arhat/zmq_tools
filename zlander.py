#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import zmq

def zmq_lander(args):
    ctx = zmq.Context()

    endpoint = args[0]

    filters = set(args[1:])
    if not filters:
        filters.add("")

    sub = ctx.socket(zmq.SUB)
    sub.connect(endpoint)
    for filter in filters:
        sub.setsockopt(zmq.SUBSCRIBE, filter)

    while True:
        print(sub.recv())

if __name__ == '__main__':
    if not sys.argv[1:]:
        print('usage: %s endpoint [filter] ...' % sys.argv[0].split('/')[-1])
        sys.exit(1)

    try:
        zmq_lander(sys.argv[1:])
    except (KeyboardInterrupt, SystemExit):
        pass

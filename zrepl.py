#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from contextlib import closing

import eventlet
from eventlet.green import zmq

import dosca

from eventlet_log import create_logger

(log, log_exc, _) = create_logger('rambler')


class ZReplicator(object):
    def __init__(self, config):
        self.config = config
        self.ctx = None

    def run(self):
        self.ctx = zmq.Context()

        publishers = self.config['PUBLISHERS']
        multiply = self.config['MULTIPLY']
        bind_host = self.config['BIND_HOST']
        port = self.config['START_PORT']

        pool = eventlet.greenpool.GreenPool(size=len(publishers))
        for publisher in publishers:
            pub_sock = self.ctx.socket(zmq.PUB)
            (bind_addr, new_port) = try_bind(pub_sock, bind_host, port)
            port = new_port + 1
            pool.spawn_n(self.replicator, publisher, bind_addr,
                         pub_sock, multiply)
        pool.waitall()

    def replicator(self, listen_to, publish_to, pub_sock, multiply):
        log('Ready to listen {0} and publish to {1} multiplying on {2}'.format(
            listen_to, publish_to, multiply))
        with closing(self.ctx.socket(zmq.SUB)) as sub, closing(pub_sock) as pub:
            sub.connect(listen_to)
            sub.setsockopt(zmq.SUBSCRIBE, "")
            while True:
                msg = sub.recv()
                log('FROM {0} - TO {1} - {2}'.format(listen_to, publish_to, msg))
                for _ in xrange(multiply):
                    pub.send(msg)

def try_bind(sock, host, port):
    while 1:
        addr = 'tcp://{0}:{1}'.format(host, port)
        try:
            sock.bind(addr)
            return (addr, port)
        except zmq.ZMQError as exc:
            if exc.errno == zmq.EADDRINUSE:
                port += 1
                continue
            else:
                raise


if __name__ == '__main__':
    args = sys.argv[1:]
    if args:
        config = dosca.parse_file(args[0])
        app = ZReplicator(config)
        try:
            app.run()
        except (KeyboardInterrupt, SystemExit):
            pass
    else:
        print('usage: %s config_file ...' % sys.argv[0].split('/')[-1])
        sys.exit(1)

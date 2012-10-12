#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from contextlib import closing, nested

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
        workers = self.config['WORKERS']

        pool = eventlet.greenpool.GreenPool(size=len(publishers) * workers)


        for publisher in publishers:
            queues = [eventlet.queue.Queue() for _ in xrange(workers)]
            pool.spawn_n(self.receiver, publisher, queues)

            for queue in queues:
                pub_sock = self.ctx.socket(zmq.PUB)
                (bind_addr, new_port) = try_bind(pub_sock, bind_host, port)
                port = new_port + 1
                pool.spawn_n(self.replicator, queue, bind_addr, pub_sock, multiply)

                pool.waitall()

    def receiver(self, listen_to, queues):
        log('Ready to listen {0}'.format(listen_to))
        workers = len(queues)
        with closing(self.ctx.socket(zmq.SUB)) as sub:
            sub.connect(listen_to)
            sub.setsockopt(zmq.SUBSCRIBE, "")
            while True:
                 msg = sub.recv()
                 log('RECV MSG FROM {0} - {1}, repl to {2} workers'.format(listen_to, msg, workers))
                 for queue in queues:
                     queue.put(msg)


    def replicator(self, queue, bind_addr, pub_sock, multiply):
        log('Ready to replicate on {0}, multiplying {1} times'.format(bind_addr, multiply))
        with closing(pub_sock) as pub:
            while True:
                msg = queue.get()
                log('GET MSG on {0}, REPL {1} times: {2}'.format(bind_addr, multiply, msg))
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

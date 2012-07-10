import sys
import os
import signal
import struct
import gevent
assert gevent.version_info > (1, 0, 0, 0), "Need gevent 1.0.0+"

from gevent import sleep, spawn, spawn_later, Greenlet
from gevent import select, socket
from gevent.server import StreamServer
from gevent.socket import create_connection, gethostbyname
from socketpool import ConnectionPool, TcpConnector

import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(msg)s")
logger = logging.getLogger(__file__)
log = logger.debug

class Socks5Server(StreamServer):

    HOSTCACHE = {}
    HOSTCACHETIME = 1800

    def __init__(self, *args, **kw):
        super(Socks5Server, self).__init__(*args, **kw)
        self.remote_pool = ConnectionPool(factory=TcpConnector,
                max_conn=None,
                max_size=600,
                max_lifetime=300,
                backend="gevent")

        def log_tcp_pool_size(s):
            log("ConnPool size: %d, alive: %d" % (self.remote_pool.size(),
                self.remote_pool.alive()))
            spawn_later(10, s, s)

        def log_dns_pool_size(s):
            log("DNSPool size: %d" % len(self.HOSTCACHE))
            spawn_later(10, s, s)

        spawn_later(10, log_tcp_pool_size, log_tcp_pool_size)
        spawn_later(10, log_dns_pool_size, log_dns_pool_size)

    def close(self):
        self.remote_pool.release_all()
        super(Socks5Server, self).close()

    def handle(self, sock, address):
        rfile = sock.makefile('rb', -1)
        try:
            log('socks connection from ' + str(address))

            # 1. Version
            sock.recv(262)
            sock.send(b"\x05\x00")

            # 2. Request
            data = rfile.read(4)
            mode = ord(data[1])
            addrtype = ord(data[3])

            if addrtype == 1:       # IPv4
                addr = socket.inet_ntoa(rfile.read(4))
            elif addrtype == 3:     # Domain name
                domain = rfile.read(ord(sock.recv(1)[0]))
                addr = self.handle_dns(domain)

            port = struct.unpack('>H', rfile.read(2))

            if mode == 1:  # 1. Tcp connect
                try:
                    remote = self.remote_pool.get(host=addr, port=port[0])
                    reply = b"\x05\x00\x00\x01" + socket.inet_aton(addr) + \
                                struct.pack(">H", port[0])
                    sock.send(reply)
                    log('Begin data, %s:%s' % (addr, port[0]))
                    # 3. Transfering
                    l1 = spawn(self.handle_tcp, sock, remote)
                    l2 = spawn(self.handle_tcp, remote, sock)
                    gevent.joinall((l1, l2))
                    self.remote_pool.release_connection(remote)

                except socket.error:
                    log('Conn refused, %s:%s' % (addr, port[0]))
                    # Connection refused
                    reply = b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00'
                    sock.send(reply)
                    raise

            else:
                reply = b"\x05\x07\x00\x01"  # Command not supported
                sock.send(reply)

        except socket.error:
            pass
        finally:
            log("Close handle")
            rfile.close()
            sock._sock.close()
            sock.close()

    def handle_dns(self, domain):

        if domain not in self.HOSTCACHE:
            log('Resolving ' + domain)
            addr = gethostbyname(domain)
            self.HOSTCACHE[domain] = addr
            spawn_later(self.HOSTCACHETIME,
                    lambda a: self.HOSTCACHE.pop(a, None), domain)
        else:
            addr = self.HOSTCACHE[domain]
            log('Hit resolv %s -> %s in cache' % (domain, addr))

        return addr

    def handle_tcp(self, fr, to):
        try:
            while to.send(fr.recv(4096)) > 0:
                continue
        except socket.error:
            pass


def main():

    listen = ("0.0.0.0", 1080)

    server = Socks5Server(listen)

    def kill():
        logger.info("kill triggered")
        server.close()
        spawn(lambda: (sleep(2) is os.closerange(3, 1024)))

    gevent.signal(signal.SIGTERM, kill)
    gevent.signal(signal.SIGQUIT, kill)
    gevent.signal(signal.SIGINT, kill)
    server.start()
    logger.info("Listening at %s" % str(listen))
    gevent.run()

if __name__ == "__main__":
    main()

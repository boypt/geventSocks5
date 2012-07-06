import sys
import os
import signal
import struct
import gevent
assert gevent.version_info > (1, 0, 0, 0), "Need gevent 1.0.0+"

from gevent import sleep, spawn
from gevent import select, socket
from gevent.server import StreamServer
from gevent.socket import create_connection, gethostbyname

import logging

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(msg)s")
logger = logging.getLogger(__file__)
log = logger.debug


class Socks5Server(StreamServer):

    HOSTCACHE = {}
    HOSTCACHETIME = 1800

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

            try:
                if mode == 1:  # 1. Tcp connect
                    reply = b"\x05\x00\x00\x01" + socket.inet_aton(addr) + \
                                struct.pack(">H", port[0])
                else:
                    reply = b"\x05\x07\x00\x01"  # Command not supported

            except socket.error:
                # Connection refused
                reply = b'\x05\x05\x00\x01\x00\x00\x00\x00\x00\x00'

            sock.send(reply)

            # 3. Transfering
            if reply[1] == '\x00':  # Success
                if mode == 1:    # 1. Tcp connect
                    log('Begin data, ' + str(address))
                    sock.setblocking(0)
                    self.handle_tcp(sock, (addr, port[0]))

        except socket.error:
            log('socket error', exc_info=1)

        finally:
            rfile.close()
            sock._sock.close()
            sock.close()

    def handle_dns(self, domain):

        log("Cache len: %d" % len(self.HOSTCACHE))

        if domain not in self.HOSTCACHE:
            log('Resolving ' + domain)
            addr = gethostbyname(domain)
            self.HOSTCACHE[domain] = addr
            spawn(lambda a: (sleep(seconds=self.HOSTCACHETIME,
                ref=False) is self.HOSTCACHE.pop(a, None)), domain)
        else:
            addr = self.HOSTCACHE[domain]
            log('Hit resolv %s -> %s in cache' % (domain, addr))

        return addr

    def handle_tcp(self, sock, rm):
        remote = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        remote.connect(rm)
        log('TCP connected, ' + rm[0])
        remote.setblocking(0)
        fdset = [sock, remote]
        try:
            while True:
                r, w, e = select.select(fdset, [], [], timeout=30)
                if sock in r:
                    if remote.send(sock.recv(4096)) <= 0:
                        break
                if remote in r:
                    if sock.send(remote.recv(4096)) <= 0:
                        break
        finally:
            remote._sock.close()
            remote.close()


def main():

    listen = ("0.0.0.0", 1080)

    server = Socks5Server(listen)

    def kill():
        log("kill triggered")
        server.close()
        spawn(lambda: (sleep(3) is os.closerange(3, 1024)))

    gevent.signal(signal.SIGTERM, kill)
    gevent.signal(signal.SIGQUIT, kill)
    gevent.signal(signal.SIGINT, kill)
    server.start()
    log("Listening at %s" % str(listen))
    gevent.run()

if __name__ == "__main__":
    main()

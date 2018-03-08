"""
Sample script showing how to do local port forwarding over paramiko.

This script connects to the requested SSH server and sets up local port
forwarding (the openssh -L option) from a local port through a tunneled
connection to a destination reachable from the SSH server machine.
"""

import select

try:
    import SocketServer
except ImportError:
    import socketserver as SocketServer

import paramiko

SSH_PORT = 22


class ForwardServer(SocketServer.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        print(self.request.getpeername())
        try:
            chan = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.chain_host, self.chain_port),
                                                   ('localhost', 8888))
        except Exception as e:
            print('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                              self.chain_port,
                                                              repr(e)))
            return
        if chan is None:
            print('Incoming request to %s:%d was rejected by the SSH server.' %
                    (self.chain_host, self.chain_port))
            return

        print('Connected!  Tunnel open %r -> %r -> %r' % (self.request.getpeername(),
                                                            chan.getpeername(), (self.chain_host, self.chain_port)))
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)

        peername = self.request.getpeername()
        chan.close()
        self.request.close()
        print('Tunnel closed from %r' % (peername,))


def forward_tunnel(local_port, remote_port, transport):

    class SubHandler(Handler):
        chain_host = 'localhost'
        chain_port = remote_port
        ssh_transport = transport

    ForwardServer(('127.0.0.1', local_port), SubHandler).serve_forever()


def forward(local_port, remote_host, remote_port, user_name, key_filename):

    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())

    print('Connecting to ssh host %s:%d ...' % (remote_host, remote_port))

    client.connect(hostname=remote_host
                   , port=22
                   , username=user_name
                   , key_filename=key_filename)

    print('Now forwarding port %d to %s:%d ...' % (local_port, remote_host, remote_port))

    try:
        forward_tunnel(local_port, remote_port, client.get_transport())
    except KeyboardInterrupt:
        print('C-c: Port forwarding stopped.')

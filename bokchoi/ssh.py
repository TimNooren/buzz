"""
Sample script showing how to do local port forwarding over paramiko.

This script connects to the requested SSH server and sets up local port
forwarding (the openssh -L option) from a local port through a tunneled
connection to a destination reachable from the SSH server machine.
"""

import select
import os
import socketserver

import paramiko

SSH_PORT = 22


class Handler(socketserver.BaseRequestHandler):

    def __init__(self, request, client_address, server):
        super(Handler, self).__init__(request, client_address, server)

    def handle(self):

        channel = self.ssh_transport.open_channel('direct-tcpip',
                                                 ('localhost', 8888),
                                                 ('localhost', self.host_port))
        if not channel:
            print('Incoming request was rejected')
            return

        while True:
            r, _, _ = select.select([self.request, channel], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if not data:
                    break
                channel.send(data)
            if channel in r:
                data = channel.recv(1024)
                if not data:
                    break
                self.request.send(data)

        channel.close()
        self.request.close()


def forward(local_port, remote_host, user_name, key_filename):
    """ Sets up port forwarding to remote host
    :param local_port:              Local port to forward to
    :param remote_host:             Remote host
    :param user_name:               User to use in ssh connection
    :param key_filename:            Private key for ssh connection
    :return:                        -
    """
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())

    print('Connecting to ssh host {}:{} ...'.format(remote_host, 8888))

    client.connect(hostname=remote_host
                   , port=22
                   , username=user_name
                   , key_filename=key_filename)

    class SubHandler(Handler):
        ssh_transport = client.get_transport()
        host_port = local_port

    server = socketserver.ThreadingTCPServer(('localhost', local_port), SubHandler)
    server.daemon_threads = True
    server.allow_reuse_address = True

    try:
        print('Notebook running at: http://localhost:8888')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Connection closed')


def get_ssh_keys(project_id):
    """Get private and public keys. Create if not exists."""
    key_file = os.path.expanduser('~/.ssh/' + project_id)
    try:
        priv = paramiko.RSAKey.from_private_key_file(key_file)
    except FileNotFoundError:
        priv = paramiko.RSAKey.generate(2048)
        priv.write_private_key_file(key_file)
    pub = priv.get_base64()
    return pub

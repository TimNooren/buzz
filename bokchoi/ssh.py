"""
Module contains functions related to SSH.

Most of this script is adapted from:
https://github.com/paramiko/paramiko/blob/master/demos/forward.py
"""
import webbrowser
import select
import os
import socketserver

from paramiko import RSAKey, SSHClient, AutoAddPolicy
from paramiko.ssh_exception import SSHException

from bokchoi import common


class ForwardServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


class Handler(socketserver.BaseRequestHandler):

    ssh_transport = None
    host_port = None

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
    client = SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(AutoAddPolicy())

    print('Connecting to ssh host {}:{} ...'.format(remote_host, 8888))

    common.retry(client.connect
                 , SSHException
                 , hostname=remote_host
                 , port=22
                 , username=user_name
                 , key_filename=key_filename)

    class SubHandler(Handler):
        ssh_transport = client.get_transport()
        host_port = local_port

    server = ForwardServer(('localhost', local_port), SubHandler)

    try:
        webbrowser.open('http://localhost:{}'.format(local_port))
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()
        server.server_close()
        print('Connection closed')


def get_ssh_keys(project_id):
    """Get private and public keys. Create if not exists.
    :param project_id:          Project id used for private key name
    :return:                    -
    """
    key_file = os.path.expanduser('~/.ssh/' + project_id)
    try:
        priv = RSAKey.from_private_key_file(key_file)
    except FileNotFoundError:
        print('Could not find private key. Creating one.')
        priv = RSAKey.generate(2048)
        priv.write_private_key_file(key_file)
    pub = priv.get_base64()
    return pub

#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EC2 spot instances
"""
import os
from base64 import b64encode

from bokchoi import utils
from bokchoi.ssh import SSH
from bokchoi.aws import common

USER_DATA = """#!/bin/bash

# Install aws-cli
sudo curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
python3 -c "import zipfile; zf = zipfile.ZipFile('/awscli-bundle.zip'); zf.extractall('/');"
sudo chmod u+x /awscli-bundle/install
python3 /awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

# Download project zip
aws s3 cp s3://{bucket}/{package} /tmp/
python3 -c "import zipfile; zf = zipfile.ZipFile('/tmp/{package}'); zf.extractall('/tmp/');"

# Install pip3 and install requirements.txt from project zip if included
curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3
[ -f /tmp/requirements.txt ] && pip3 install -r /tmp/requirements.txt

if [ "{notebook}" = "True" ]
then
    #Add public key
    echo "ssh-rsa {public_key}" >> /home/ubuntu/.ssh/authorized_keys
    #Install Jupyter
    pip3 install jupyter
    pip3 install jupyterlab
    jupyter serverextension enable --py jupyterlab --sys-prefix
    pip3 install tornado==4.5.2
    echo "c.NotebookApp.token = u''" >> ~/.jupyter/jupyter_notebook_config.py
    jupyter lab --no-browser --allow-root --ip=0.0.0.0 --port=8888 --NotebookApp.token=
else
    # Run app
    cd /tmp
    python3 {app}
    aws s3 cp /var/log/cloud-init-output.log s3://{bucket}/cloud-init-output.log
    if [ "{shutdown}" = "True" ]
    then
        shutdown -h now
    fi
fi
"""

DEFAULT_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""

DEFAULT_POLICY = """{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Action": [
        "s3:Get*",
        "s3:List*",
        "s3:Put*"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::{bucket}/*"
    }}
  ]
}}"""


class EC2:
    """Create EC2 object which can be used to schedule jobs"""
    def __init__(self, project_name, config):

        self._name = project_name
        self.config = config

        self.launch_spec = config.ec2['LaunchSpecification']
        self.subnet = common.get_subnet(self.launch_spec['SubnetId'])

        self.project_id = utils.create_project_id(project_name, common.get_aws_account_id())
        self.package_name = 'bokchoi-' + project_name + '.zip'

        if self.config.connect:
            self.ssh = SSH(self.project_id)

    @property
    def default_config(self):
        return {'Platform': 'EC2',
                'Notebook': True,
                'Entrypoint': '',
                'EC2': {
                    'Region': '',
                    'SpotPrice': '',
                    'LaunchSpecification': {
                      'ImageId': '',
                      'InstanceType': '',
                      'SubnetId': ''
                    }
                  }
                }

    def deploy(self, path=''):
        """Zip package and deploy to S3"""

        bucket_name = common.create_bucket(self.config.ec2['Region'], self.project_id)

        package, fingerprint = utils.zip_package(path or os.getcwd(), self.config.requirements)
        common.upload_to_s3(bucket_name, package, self.package_name, fingerprint)

        policies = self.create_policies(self.config.ec2.get('CustomPolicy'))

        self.create_default_role_and_profile(policies)

        common.create_security_group(self.project_id
                                     , self.project_id
                                     , self.subnet.vpc_id
                                     , {'CidrIp': utils.get_my_ip() + '/32'
                                        , 'FromPort': 22
                                        , 'ToPort': 22
                                        , 'IpProtocol': 'tcp'}
                                     )

    def undeploy(self, dryrun):
        """Deletes all policies, users, and instances permanently"""

        common.cancel_spot_request(self.project_id, dryrun)

        for instance in common.get_instances(self.project_id):
            common.terminate_instance(instance, dryrun)

        common.delete_bucket(self.project_id, dryrun)

        for policy in common.get_policies(self.project_id):
            common.delete_policy(policy, dryrun)

        for instance_profile in common.get_instance_profiles(self.project_id):
            common.delete_instance_profile(instance_profile, dryrun)

        for role in common.get_roles(self.project_id):
            common.delete_role(role, dryrun)

        for group in common.get_security_groups(self.project_id):
            common.delete_security_group(group, dryrun)

    def run(self):
        """Create EC2 machine with given AMI and instance settings"""

        if self.config.connect:
            public_key = self.ssh.public_key
        else:
            public_key = ''

        if self.config.connect:
            security_group = next(common.get_security_groups(self.project_id, self.project_id))
            if self.launch_spec.get('SecurityGroupIds'):
                self.launch_spec['SecurityGroupIds'] += [security_group.group_id]
            else:
                self.launch_spec['SecurityGroupIds'] = [security_group.group_id]
        print(public_key)
        user_data = USER_DATA.format(bucket=self.project_id
                                     , package=self.package_name
                                     , app=self.config.app
                                     , public=public_key
                                     , shutdown=self.config.shutdown
                                     , notebook=self.config.connect
                                     , public_key=public_key)

        self.launch_spec['UserData'] = b64encode(user_data.encode('ascii')).decode('ascii')
        self.launch_spec['IamInstanceProfile'] = {'Name': self.project_id}

        common.request_spot_instances(self.project_id, self.launch_spec, self.config.ec2['SpotPrice'])

    def create_default_role_and_profile(self, policies):
        """ Creates default role and instance profile for EC2 deployment.
        :param policies:                Policies to attach to default role
        """
        role_name = self.project_id
        common.create_role(role_name, DEFAULT_TRUST_POLICY, *policies)
        common.create_instance_profile(role_name, role_name)

    def create_policies(self, custom_policy):
        """Creates policies for EMR related tasks"""
        policies = []

        # declare default policy settings
        default_policy_name = self.project_id + '-default-policy'
        default_policy_document = DEFAULT_POLICY.format(bucket=self.project_id)
        common.create_policy(default_policy_name, default_policy_document)

        policies.append(next(common.get_policies(default_policy_name)))

        if custom_policy:
            print('Creating custom policy')

            custom_policy_name = self.project_id + '-custom-policy'
            common.create_policy(custom_policy_name, custom_policy)
            policies.append(next(common.get_policies(custom_policy_name)))

        return policies

    def connect(self, local_port, remote_port):
        """Set up port forwarding to remote server"""
        instance = next(common.get_instances(self.project_id))
        instance_ip = instance.public_ip_address or instance.private_ip_address
        self.ssh.forward(local_port or 8888, instance_ip, remote_port or 8888, 'ubuntu')

    def stop(self, dryrun=False):
        """Stop all running instances"""
        common.cancel_spot_request(self.project_id, dryrun)

        for instance in common.get_instances(self.project_id):
            common.terminate_instance(instance, dryrun)

    def status(self):
        """Status of current deployment"""
        print('\nStatus:')
        for instance in common.get_instances(self.project_id):
            print('\t' + instance.instance_id + ' : ' + instance.state['Name'])
        print()
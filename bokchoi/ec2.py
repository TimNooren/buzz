#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EC2 spot instances
"""
import os
from base64 import b64encode

from bokchoi import common
from bokchoi.scheduler import Scheduler

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

# Run app
cd /tmp
python3 -c "import {app}; {app}.{entry}();"
aws s3 cp /var/log/cloud-init-output.log s3://{bucket}/cloud-init-output.log
shutdown -h now
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


class EC2(object):
    """Create EC2 object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        self.settings = settings
        self.project_name = project
        self.requirements = settings.get('Requirements')
        self.region = settings.get('Region')
        self.entry_point = settings.get('EntryPoint')
        self.launch_config = settings.get('EC2')
        self.custom_policy = settings.get('CustomPolicy')

        aws_account_id = common.get_aws_account_id()
        self.project_id = common.create_project_id(project, aws_account_id)
        self.package_name = 'bokchoi-' + self.project_name + '.zip'

        self.schedule = settings.get('Schedule')

    def deploy(self):
        """Zip package and deploy to S3"""

        bucket_name = common.create_bucket(self.region, self.project_id)

        cwd = os.getcwd()
        package, fingerprint = common.zip_package(cwd, self.requirements)
        common.upload_to_s3(bucket_name, package, self.package_name, fingerprint)

        policies = self.create_policies(self.custom_policy)

        self.create_default_role_and_profile(policies)

        if self.settings.get('Schedule'):
            scheduler = Scheduler(self.project_id
                                  , self.project_name
                                  , self.settings.get('Schedule')
                                  , self.settings.get('Requirements'))
            scheduler.deploy()

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""

        common.cancel_spot_request(self.project_id)
        common.terminate_instances(self.project_id)

        common.delete_bucket(self.project_id)

        for policy in common.get_policies(self.project_id):
            common.delete_policy(policy)

        for instance_profile in common.get_instance_profiles(self.project_id):
            common.delete_instance_profile(instance_profile)

        for role in common.get_roles(self.project_id):
            common.delete_role(role)

        scheduler = Scheduler(self.project_id
                              , self.project_name
                              , self.settings.get('Schedule')
                              , self.settings.get('Requirements'))
        scheduler.undeploy()

    def run(self):
        """Create EC2 machine with given AMI and instance settings"""
        print("Starting EC2 instance")

        bucket_name = self.project_id

        app, entry = self.entry_point.split('.')
        user_data = USER_DATA.format(bucket=bucket_name, package=self.package_name, app=app, entry=entry)
        self.launch_config['LaunchSpecification']['UserData'] = b64encode(user_data.encode('ascii')).decode('ascii')

        self.launch_config['LaunchSpecification']['IamInstanceProfile'] = {'Name': self.project_id + '-default-role'}

        common.request_spot_instances(self.project_id, self.launch_config)

    def create_default_role_and_profile(self, policies):
        """ Creates default role and instance profile for EC2 deployment.
        :param policies:                Policies to attach to default role
        """
        role_name = self.project_id + '-default-role'
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
            policies.append(next(common.get_policies(default_policy_name)).arn)

        return policies

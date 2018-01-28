#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EMR jobs
"""

import os
import sys
import time
import boto3

from bokchoi import common


class EMR(object):
    """Create EMR object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        self.settings = settings
        self.project_name = project

        aws_account_id = common.get_aws_account_id()
        self.project_id = common.create_project_id(project, aws_account_id)
        self.job_flow_id = None

    def schedule(self):
        """Schedule task"""
        if self.settings.get('Schedule'):

            from bokchoi.scheduler import Scheduler

            scheduler = Scheduler(self.project_id
                                  , self.project_name
                                  , self.settings.get('Schedule')
                                  , self.settings.get('Requirements'))
            scheduler.deploy()

    def deploy(self):
        """Zip package and deploy to S3 so it can be used by EMR"""
        bucket_name = common.create_bucket(self.settings['Region'], self.project_id)

        cwd = os.getcwd()
        package, fingerprint = common.zip_package(cwd, self.settings.get('Requirements'))

        package_name = 'bokchoi-' + self.project_name + '.zip'
        common.upload_to_s3(bucket_name, package, package_name, fingerprint)
        self.schedule()

    def run(self):
        """Create Spark cluster and run specified job"""
        emr_client = boto3.client('emr')
        self.start_spark_cluster(emr_client)
        self.step_prepare_env(emr_client)
        self.step_spark_submit(emr_client)

    def undeploy(self, dryrun):
        """Deletes all policies, users, and instances permanently"""

        common.cancel_spot_request(self.project_id, dryrun)
        common.terminate_instances(self.project_id, dryrun)

        for pol in common.get_policies(self.project_id):
            common.delete_policy(pol, dryrun)

        for prof in common.get_instance_profiles(self.project_id):
            common.delete_instance_profile(prof, dryrun)

        for role in common.get_roles(self.project_id):
            common.delete_role(role, dryrun)

        # remove s3 bucket
        common.delete_bucket(self.project_id, dryrun)

        from bokchoi.scheduler import Scheduler

        scheduler = Scheduler(self.project_id
                              , self.project_name
                              , self.settings.get('Schedule')
                              , self.settings.get('Requirements'))
        scheduler.undeploy(dryrun)

    def start_spark_cluster(self, emr_client):
        """
        Start Spark cluster based on configuration given in settings
        """

        launch_spec = self.settings['EMR']['LaunchSpecification']

        instance_type = launch_spec['InstanceType']
        instance_count = self.settings['EMR']['InstanceCount']

        instances = {'KeepJobFlowAliveWhenNoSteps': False
                     , 'TerminationProtected': False
                     , 'Ec2SubnetId': self.settings['EMR']['LaunchSpecification']['SubnetId']
                     , 'InstanceGroups': [
                            {
                                'Name': 'EmrMaster',
                                'Market': 'SPOT',
                                'InstanceRole': 'MASTER',
                                'BidPrice': self.settings['EMR']['SpotPrice'],
                                'InstanceType': instance_type,
                                'InstanceCount': 1
                            },
                            {
                                'Name': 'EmrCore',
                                'Market': 'SPOT',
                                'InstanceRole': 'CORE',
                                'BidPrice': self.settings['EMR']['SpotPrice'],
                                'InstanceType': instance_type,
                                'InstanceCount': instance_count - 1
                            }
                        ]
                     }

        additional_sgs = launch_spec.get('AdditionalSecurityGroups')
        if additional_sgs:
            instances['AdditionalMasterSecurityGroups'] = additional_sgs
            instances['AdditionalSlaveSecurityGroups'] = additional_sgs

        response = emr_client.run_job_flow(
            Name=self.project_id,
            LogUri="s3://{}/spark/".format(self.project_id),
            ReleaseLabel=self.settings['EMR']['Version'],
            Instances=instances,
            Configurations=[
                {"Classification": "spark-env"
                 , "Properties": {}
                 , "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {
                                "PYSPARK_PYTHON": "python34"
                            },
                            "Configurations": []
                        }
                    ]
                }
            ],
            Tags=[{'Key': 'bokchoi-id', 'Value': self.project_id}],
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
        )
        # parse EMR response to check if successful
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            print("Error creating: (status code {})".format(response_code))
            sys.exit(1)

        print("Created Spark cluster with job {}".format(self.job_flow_id))

    def step_prepare_env(self, emr_client):
        """Copies files from S3 and unzips them"""
        package_name = 'bokchoi-' + self.project_name + '.zip'
        s3_package_uri = 's3://{bucket}/{key}'.format(bucket=self.project_id, key=package_name)
        root_dir = '/home/hadoop/'

        emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', s3_package_uri, root_dir]
                    }
                },
                {
                    'Name': 'setup - unzip files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['unzip', root_dir + package_name, '-d', root_dir]
                    }
                },
                {
                    'Name': 'setup - install python dependencies',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'pip-3.4', 'install', '-r', root_dir + 'requirements.txt']
                    }
                }
            ]
        )

    def step_spark_submit(self, emr_client):
        """Submit spark job given by user"""
        py3_env = 'PYSPARK_PYTHON=/usr/bin/python3'
        script = self.settings['EntryPoint']

        emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--conf', py3_env, '/home/hadoop/' + script]
                    }
                }
            ]
        )
        print("Added step 'spark-submit'")
        time.sleep(1)  # Prevent ThrottlingException


import os
import zipfile
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

from bokchoi import common

session = boto3.Session()

s3_client = session.client('s3')
s3_resource = session.resource('s3')

lambda_client = session.client('lambda')

events_client = session.client('events')

SCHEDULER_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


SCHEDULER_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
       "ec2:DescribeImages",
       "ec2:DescribeSubnets",
       "ec2:RequestSpotInstances",
       "ec2:TerminateInstances",
       "ec2:DescribeInstanceStatus",
       "ec2:DescribeSecurityGroups",
       "ec2:DescribeSpotInstanceRequests",
       "ec2:CreateTags",
       "iam:PassRole",
       "elasticmapreduce:RunJobFlow",
       "elasticmapreduce:AddJobFlowSteps"
        ],
    "Resource": ["*"]
  }]
}"""

EVENT_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "*"
    }
  ]
}"""

EVENT_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


class Scheduler:

    def __init__(self, project_id, project_name, schedule, requirements=None):

        self.project_id = project_id
        self.project_name = project_name

        self.schedule = schedule

        self.bucket_name = project_id

        self.requirements = requirements

        self.function_name = project_id + '-scheduler'
        self.rule_name = project_id + '-rule'

    def deploy(self):

        file_object = self.zip_package(self.requirements)

        zip_file_name = 'bokchoi-scheduler.zip'
        common.upload_to_s3(self.bucket_name, file_object, zip_file_name, fingerprint='n/a')

        scheduler_role = self.create_role(self.project_id
                                          , self.project_id + '-scheduler-role', SCHEDULER_TRUST_POLICY
                                          , self.project_id + '-scheduler-policy', SCHEDULER_POLICY)

        lambda_function = common.retry(self.create_lambda_function
                                       , ClientError
                                       , project_id=self.project_id
                                       , project_name=self.project_name
                                       , role=scheduler_role
                                       , bucket_name=self.bucket_name
                                       , zip_file_name=zip_file_name)

        event_role = self.create_role(self.project_id
                                      , self.project_id + '-event-role', EVENT_TRUST_POLICY
                                      , self.project_id + '-event-policy', EVENT_POLICY)

        self.create_cloudwatch_rule(self.schedule, event_role.arn, lambda_function['FunctionArn'])

    def undeploy(self, dryrun):

        self.delete_lambda_function(dryrun)
        self.delete_cloudwatch_rule(dryrun)

    def create_role(self, project_id, role_name, trust_policy, policy_name, policy):
        common.create_policy(policy_name, policy)
        scheduler_policy = next(common.get_policies(project_id, pattern=policy_name))

        return common.create_role(role_name, trust_policy, scheduler_policy)

    def zip_package(self, requirements=None):

        from bokchoi import ec2, emr, common, scheduler_lambda

        file_object = BytesIO()

        with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            zip_file.write(scheduler_lambda.__file__, 'scheduler_lambda.py')
            zip_file.write(common.__file__, 'bokchoi/common.py')
            zip_file.write(ec2.__file__, 'bokchoi/ec2.py')
            zip_file.write(emr.__file__, 'bokchoi/emr.py')
            zip_file.write(emr.__file__, 'bokchoi/scheduler.py')
            zip_file.write('/'.join((os.getcwd(), 'bokchoi_settings.json')), 'bokchoi_settings.json')

            zip_file.writestr('__init__.py', '')
            info = zipfile.ZipInfo('bokchoi/__init__.py')
            info.external_attr = 0o777 << 16  # give full access to included file
            zip_file.writestr(info, '')

            requirements = requirements or ''
            zip_file.writestr('requirements.txt', '\n'.join(requirements))

        file_object.seek(0)

        return file_object

    def create_lambda_function(self, project_id, project_name, role, bucket_name, zip_file_name):

        try:
            lambda_client.create_function(FunctionName=self.function_name
                                          , Runtime='python3.6'
                                          , Handler='scheduler_lambda.run'
                                          , Role=role.arn
                                          , Code={'S3Bucket': bucket_name
                                                  , 'S3Key': zip_file_name}
                                          , Timeout=30
                                          , Environment={'Variables': {'project_name': project_name}}
                                          , Tags={'bokchoi-id': project_id})
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print('Scheduler already exists')
            else:
                raise e

        return lambda_client.get_function_configuration(FunctionName=self.function_name)

    def delete_lambda_function(self, dryrun):
        """ Deletes scheduler lambda function"""

        if dryrun:
            print('Dryrun flag set. Would have deleted scheduler')
            return

        try:
            lambda_client.delete_function(FunctionName=self.function_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print('Function does not exist')
            else:
                raise e

    def create_cloudwatch_rule(self, schedule, role_arn, function_arn):
        """ Creates Cloudwatch rule (event) that will trigger lambda function
        :param schedule:                Event schedule
        :param role_arn:                ARN of role to assign to rule
        :param function_arn:            ARN of Lambda function to trigger
        """
        print('Scheduling job using ' + schedule)

        common.retry(events_client.put_rule
                     , ClientError
                     , Name=self.rule_name
                     , ScheduleExpression=schedule
                     , State='ENABLED'
                     , RoleArn=role_arn)

        events_client.put_targets(Rule=self.rule_name
                                  , Targets=[{'Arn': function_arn, 'Id': '0'}])

        try:
            lambda_client.add_permission(
                FunctionName=self.function_name,
                StatementId='0',
                Action='lambda:InvokeFunction',
                Principal='events.amazonaws.com'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceConflictException':
                print('Scheduler rule already exists')
            else:
                raise e

    def delete_cloudwatch_rule(self, dryrun):
        """ Delete Cloudwatch rule (event). First removes all targets"""

        if dryrun:
            print('Dryrun flag set. Would have deleted cloudwatch rule')
            return

        try:
            events_client.remove_targets(Rule=self.rule_name
                                         , Ids=['0'])
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print('No targets to remove from rule')
            else:
                raise e

        try:
            events_client.delete_rule(Name=self.rule_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                print('Cloudwatch rule does not exist')
            else:
                raise e

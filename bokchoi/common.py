
from time import sleep
from io import BytesIO
import zipfile
import os
import json
import hashlib
import requests

import boto3
from botocore.exceptions import ClientError

session = boto3.Session()

ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')

iam_client = session.client('iam')
iam_resource = session.resource('iam')

s3_client = session.client('s3')
s3_resource = session.resource('s3')


def get_aws_account_id():
    """ Returns AWS account ID"""
    response = ec2_client.describe_security_groups(GroupNames=['Default'])
    return response['SecurityGroups'][0]['OwnerId']


def create_bucket(region, bucket_name):
    """ Creates bucket to store application packages
    :param region:                  Region to create bucket in
    :param bucket_name:             Name of bucket
    :return:                        Name of bucket
    """
    try:
        s3_resource.create_bucket(Bucket=bucket_name
                                  , CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError as exception:
        if exception.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print('Bucket already exists and owned by you, continuing')
        else:
            raise exception
    else:
        print('Created bucket: ' + bucket_name)

    return bucket_name


def upload_to_s3(bucket_name, file_object, file_name, fingerprint):
    """ Uploads file to S3
    :param bucket_name:                 Bucket name
    :param file_object:                 File to upload
    :param file_name:                   Name of zip file in S3
    :param fingerprint:                 Fingerprint of file_object
    """
    bucket = s3_resource.Bucket(bucket_name)

    try:
        cur_fingerprint = bucket.Object(file_name).metadata.get('fingerprint')
    except ClientError as e:
        if e.response['Error']['Message'] == 'Not Found':
            print('No package deployed yet. Uploading.')
        else:
            raise e
    else:
        if cur_fingerprint == fingerprint:
            print('Local package matches deployed. Not uploading.')
            return
        else:
            print('Local package does not match deployed. Uploading')

    bucket.put_object(Body=file_object, Key=file_name, Metadata={'fingerprint': fingerprint})


def retry(func, exc, **kwargs):
    """ Retries boto3 function call in case a ClientError occurs
    :param func:                    Function to call
    :param exc:                     Exception to catch
    :param kwargs:                  Parameters to pass to function
    :return:                        Function response
    """
    for _ in range(60):
        try:
            response = func(**kwargs)
            return response
        except exc:
            sleep(1)

    raise TimeoutError()


def create_security_group(group_name, project_id, *rules):
    try:
        group = ec2_resource.create_security_group(
            Description='Bokchoi default security group',
            GroupName=group_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidGroup.Duplicate':
            print('Security group already exists ' + group_name)
            return
        else:
            raise e

    group.create_tags(Tags=[{'Key': 'bokchoi-id', 'Value': project_id}])
    for rule in rules:
        group.authorize_ingress(**rule)
    print('Created security group: ' + group_name)
    return group


def get_security_groups(project_id, *group_names):
    response = ec2_client.describe_security_groups(
        Filters=[
            {
                'Name': 'tag-key',
                'Values': ['bokchoi-id']
            },
            {
                'Name': 'tag-value',
                'Values': [project_id]
            }
        ],
        GroupNames=[*group_names]
    )

    for group in response['SecurityGroups']:
        yield ec2_resource.SecurityGroup(group['GroupId'])


def delete_security_group(group, dryrun=True):

    if dryrun:
        print('Dryrun flag set. Would have deleted security group ' + group.group_name)
        return

    group_name = group.group_name
    group.delete()

    print('Deleted security group ' + group_name)


def get_my_ip():
    return requests.get('https://api.ipify.org/').text


def create_instance_profile(profile_name, role_name=None):
    """ Creates IAM instance profile
    :param profile_name:            Name of profile to be created
    :param role_name:               Name of role to attach to instance profile
    :return:                        API response
    """
    try:
        create_instance_profile_response = iam_client.create_instance_profile(
            InstanceProfileName=profile_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Instance profile already exists ' + profile_name)
            return
        else:
            raise e

    if role_name:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name
        )
    print('Created instance profile: ' + profile_name)
    return create_instance_profile_response['InstanceProfile']


def create_policy(policy_name, document):
    """ Creates IAM policy
    :param policy_name:             Name of policy to create
    :param document:                Policy document associated with policy
    """
    try:
        iam_client.create_policy(PolicyName=policy_name
                                 , PolicyDocument=document)
        print('Created policy: ' + policy_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Policy already exists ' + policy_name)
        else:
            raise e


def create_role(role_name, trust_policy, *policies):
    """ Creates IAM role
    :param role_name:               Name of role to create
    :param trust_policy:            Trust policy to associate with role
    :param policies:                Policies to attach to role
    :return:                        API response
    """
    try:
        iam_client.create_role(RoleName=role_name
                               , AssumeRolePolicyDocument=trust_policy)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Role already exists ' + role_name)
            return iam_resource.Role(role_name)
        else:
            raise e

    for policy in policies:
        if not policy:
            continue
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy.arn
        )
    print('Created role: ' + role_name)
    return iam_resource.Role(role_name)


def request_spot_instances(project_id, settings):
    """ Create spot instance request
    :param project_id:              Global project id
    :param settings:                Settings to pass to request
    """
    response = ec2_client.request_spot_instances(**settings)

    spot_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    waiter = ec2_client.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(SpotInstanceRequestIds=[spot_request_id])

    ec2_client.create_tags(Resources=[spot_request_id]
                           , Tags=[{'Key': 'bokchoi-id', 'Value': project_id}])

    response = ec2_client.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_request_id])
    instance_ids = [request['InstanceId'] for request in response['SpotInstanceRequests']]

    ec2_client.create_tags(Resources=instance_ids, Tags=[{'Key': 'bokchoi-id', 'Value': project_id}])


def cancel_spot_request(project_id, dryrun):
    """ Cancels spot instance request. Request is found by filtering on project_id tag.
    :param project_id:              Global project id
    :param dryrun:                  If true list id's of spot requests to cancel
    """
    print('\nCancelling spot request')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(project_id)]}
               , {'Name': 'state', 'Values': ['open', 'active']}]
    response = ec2_client.describe_spot_instance_requests(Filters=filters)

    spot_request_ids = [request['SpotInstanceRequestId'] for request in response['SpotInstanceRequests']]

    if dryrun:
        print('Dryrun flag set. Would have cancelled spot requests ' + ', '.join(spot_request_ids))
        return

    try:
        ec2_client.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_request_ids)
        print('Spot requests cancelled')
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidParameterCombination':
            print('No spot requests to cancel')
        else:
            raise e


def get_instances(project_id):
    """ Returns all instances for project. Instances are found by filtering on project_id tag
    :param project_id:
    :return:
    """
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(project_id)]},
               {'Name': 'instance-state-name', 'Values': ['pending', 'running', 'stopping', 'stopped']}]
    for instance in ec2_resource.instances.filter(Filters=filters):
        yield instance


def terminate_instance(instance, dryrun=True):
    """ Terminates instance.
    :param instance:                ec2.Instance
    :param dryrun:                  If True print instance that would be terminated
    """
    print('\nTerminating instances')

    if dryrun:
        print('Dryrun flag set. Would have terminated instance: ' + instance.instance_id)
        return

    instance.terminate()
    instance.wait_until_terminated()
    print('Instance terminated')


def delete_bucket(project_id, dryrun=True):
    """ Delete Bokchoi deploy bucket. Removes all object it contains.
    :param project_id:              Global project id
    :param dryrun:                  If True list bucket that would be terminated
    """
    print('\nDelete Bucket')

    bucket = s3_resource.Bucket(project_id)

    if dryrun:
        print('Dryrun flag set. Would have deleted bucket ' + bucket.name)
        return

    try:
        bucket.objects.delete()
        bucket.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            pass
        else:
            raise e


def get_instance_profiles(project_id):
    """ Yields all instance profiles associated with deployment
    :param project_id:              Global project id
    """
    for instance_profile in iam_resource.instance_profiles.all():
        if project_id in instance_profile.instance_profile_name:
            yield instance_profile


def delete_instance_profile(instance_profile, dryrun):
    """ Deletes instance profile. First removes all roles attached to instance profile.
    :param instance_profile:        Name of instance profile
    :param: dryrun:                 If true print name of instance profile to delete
    """
    instance_profile_name = instance_profile.instance_profile_name
    print('\nDeleting Instance Profile:', instance_profile_name)

    if dryrun:
        print('Dryrun flag set. Would have deleted instance profile ' + instance_profile_name)
        return

    try:
        for role in instance_profile.roles_attribute:
            instance_profile.remove_role(RoleName=role['RoleName'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            pass
        else:
            raise e

    try:
        instance_profile.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            pass
        else:
            raise e

    print('Successfully deleted Instance Profile:', instance_profile_name)


def get_roles(project_id):
    """ Yields all IAM roles associated with deployment
    :param project_id:              Global project id
    :return:                        IAM role
    """
    for role in iam_resource.roles.all():
        if project_id in role.role_name:
            yield role


def delete_role(role, dryrun):
    """ Deletes IAM role. First detaches all polices from role
    :param role:                    Boto3 Role resource
    :param dryrun:                  If true print name of role to delete
    """
    role_name = role.role_name
    print('\nDeleting Role:', role_name)

    if dryrun:
        print('Dryrun flag set. Would have deleted role ' + role_name)
        return

    try:
        for policy in role.attached_policies.all():
            policy.detach_role(RoleName=role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('No policies to detach')
        else:
            raise e

    try:
        role.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Role does not exist')
        else:
            raise e

    print('Successfully deleted role:', role_name)


def get_policies(project_id, pattern=None):
    """ Yields all IAM policies associated with deployment
    :param project_id:              Global project id
    :param pattern:                 Pattern to return specific policies (e.g. default-policy)
    :return:                        Boto3 policy resource
    """
    for policy in iam_resource.policies.filter(Scope='Local'):

        policy_name = policy.policy_name

        if project_id not in policy_name:
            continue

        if pattern and pattern not in policy_name:
            continue

        yield policy


def delete_policy(policy, dryrun):
    """ Deletes IAM policy. First detaches all roles from policy.
    :param policy:                  Boto3 policy resource
    :param dryrun:                  If true print name of policy to delete
    """
    policy_name = policy.policy_name
    print('\nDeleting Policy:', policy_name)

    if dryrun:
        print('Dryrun flag set. Would have deleted policy ' + policy_name)
        return

    try:
        for role in policy.attached_roles.all():
            policy.detach_role(RoleName=role.role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Role for policy does not exist')
        else:
            raise e

    try:
        policy.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Policy does not exist')
        else:
            raise e

    print('Successfully deleted Policy:', policy_name)


def create_project_id(project, vendor_specific_id):
    """Creates project id by hashing vendor specific id and project name"""
    return 'bokchoi-' + hashlib.sha1((vendor_specific_id + project).encode()).hexdigest()


def load_settings(project_name):
    """ Loads settings from bokchoi_settings.json. Looking for settings under
    the project name as a key on the lowest level
    :param project_name:            Name of project
    :return:                        Settings
    """
    with open('bokchoi_settings.json', 'r') as settings_file:

        settings = json.load(settings_file)

        try:
            return settings[project_name]
        except KeyError:
            raise KeyError('No config found for {} in bokchoi_settings'.format(project_name))


def zip_package(path, requirements=None):
    """ Creates deployment package by zipping the project directory. Writes requirements to requirements.txt
    if specified in settings
    :param path:                    Path to project directory
    :param requirements:            List of python requirements
    :return:                        Zip file
    """
    file_object = BytesIO()

    rootlen = len(path) + 1

    with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for base, _, files in os.walk(path):
            for file_name in files:
                fn = os.path.join(base, file_name)
                zip_file.write(fn, fn[rootlen:])

        requirements = requirements or ''
        zip_file.writestr('requirements.txt', '\n'.join(requirements))

        fingerprint = '|'.join([str(elem.CRC) for elem in zip_file.infolist()])

    file_object.seek(0)

    return file_object, fingerprint

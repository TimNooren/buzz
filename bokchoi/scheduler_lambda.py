"""
Schedule tasks using lambda and cloudwatch
"""

import os

PROJECT = os.environ.get('project_name')


def run(event, context):

    from bokchoi import common
    from bokchoi.ec2 import EC2
    from bokchoi.emr import EMR

    settings = common.load_settings(PROJECT)

    platforms = {'EC2': EC2, 'EMR': EMR}
    platform = platforms.get(settings['Platform'])(PROJECT, settings)

    platform.run()

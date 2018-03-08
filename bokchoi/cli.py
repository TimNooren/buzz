#!/usr/bin/env python3.6
"""
Main cli program which allows execution of commands
"""
import sys
import click

from bokchoi import common
from bokchoi.ec2 import EC2
from bokchoi.emr import EMR


@click.group()
@click.argument('project')
@click.pass_context
def cli(ctx, project):
    """Starting point for cli commands"""
    settings = common.load_settings(project)

    # instantiate object based on given platform parameter in settings file
    platforms = {'EC2': EC2, 'EMR': EMR}
    ctx.obj = platforms.get(settings['Platform'])(project, settings)

    if not ctx.obj:
        click.echo("Choose a supported instance type option..")
        sys.exit(1)


@cli.command('run')
@click.pass_obj
def run(ctx):
    """Default run command"""
    ctx.run()
    click.secho('Running application', fg='green')


@cli.command('deploy')
@click.pass_obj
def deploy(ctx):
    """Default deploy command"""
    ctx.deploy()
    click.secho('Deployed', fg='green')


@cli.command('undeploy')
@click.option('--dryrun', is_flag=True, default=False, help="Print in stead of terminate")
@click.pass_obj
def undeploy(ctx, dryrun):
    """Default undeploy command"""
    ctx.undeploy(dryrun)
    click.secho('Undeployed', fg='green')


@cli.command('connect')
@click.pass_obj
def connect(ctx):
    """Default connect command"""
    ctx.connect()


@cli.command('stop')
@click.pass_obj
def stop(ctx):
    """Default stop command"""
    ctx.stop()

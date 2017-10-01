# coding: utf-8
# crowdworks-extended

from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models, settings

import httplib2
from oauth2client.client import OAuth2Credentials
from apiclient.discovery import build
from sqlalchemy.orm.exc import NoResultFound
import time


logger = get_task_logger(__name__)


@celery.task(name="redash.tasks.sync_google_group_members")
def sync_google_group_members(group_id=None, org_slug=None):
    if not settings.GOOGLE_GROUP_MEMBER_SYNC_ENABLED:
        logger.info("Google Group Member sync is disabled.")
        logger.info("GOOGLE_GROUP_MEMBER_SYNC_ENABLED={}".format(settings.GOOGLE_GROUP_MEMBER_SYNC_ENABLED))
        return

    if group_id is None and org_slug is None:
        # 全グループをチェックするジョブをenqueueする
        enqueue_all_group_sync_job()
        logger.info("finished.")
        return

    # 指定されたグループをチェックするジョブ
    logger.debug("invoked sync_google_group_members (group_id={}, org_slug={})".format(group_id, org_slug))
    org = models.Organization.get_by_slug(org_slug)
    group = models.Group.get_by_id_and_org(group_id, org=org)
    logger.info("Group(id={}, name={}) org_slug={}".format(group.id, group.name, org.slug))
    sync_memberships(group, org)
    logger.info("finished.")


def enqueue_all_group_sync_job():
    for org in models.Organization.query:
        for group in models.Group.all(org=org):
            group_info = "Group(id={}, name={}) slug={}".format(group.id, group.name, org.slug)
            if '@' in group.name:
                logger.info("enqueue job: {}".format(group_info))
                sync_google_group_members.delay(group.id, org.slug)
            else:
                logger.info("enqueue skip: {}".format(group_info))
    logger.info("enqueue done.")


def sync_memberships(group, org):
    log_prefix = "[{}:{}] ".format(org.slug, group.name)
    if '@' not in group.name:
        logger.info(log_prefix + "Group name does not contain '@' mark.")
        logger.info(log_prefix + "Skip sync members from Google Group.")
        return

    directory_service = google_directory_service()

    domains = org.google_apps_domains
    google_group_members = get_google_group_members(directory_service, group.name, domains)
    exists_members = set([m.email for m in models.Group.members(group.id)])

    logger.info(log_prefix + "google_app_domain: {}".format(', '.join(domains)))

    if exists_members == google_group_members:
        logger.info(log_prefix + "already synced ({} members)".format(len(exists_members)))
    else:
        logger.info(log_prefix + "current_exists_members: ({} members) {}".format(len(exists_members), ', '.join(exists_members)))
        logger.info(log_prefix + "google_group_members: ({} members) {}".format(len(google_group_members), ', '.join(google_group_members)))

        logger.info(log_prefix + "create or update user...")
        for add_email in (google_group_members - exists_members):
            add_member(log_prefix, org, add_email, group)

        for del_email in (exists_members - google_group_members):
            del_member(log_prefix, org, del_email, group)


def google_directory_service():
    credential = OAuth2Credentials.from_json(settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_TOKEN)
    http_auth = credential.authorize(httplib2.Http())
    credential.refresh(http_auth)
    service = build('admin', 'directory_v1', http=http_auth)
    return service


def get_google_group_members(service, group_key, domains):
    pageToken = None
    members = []
    while True:
        result = service.members().list(groupKey=group_key, pageToken=pageToken, maxResults=200).execute()
        for member in result['members']:
            if member['status'] != 'ACTIVE':
                continue

            email_domain = member['email'].split('@', 2)[1]
            if member['type'] == 'USER' and email_domain in domains:
                members.append(member['email'])
            elif member['type'] == 'GROUP':
                members.extend(get_google_group_members(service, member['email'], domains))
        if 'nextPageToken' in result:
            pageToken = result['nextPageToken']
        else:
            break
    return set(members)


def add_member(log_prefix, org, email, group):
    from redash.tasks import record_event

    try:
        user = models.User.get_by_email_and_org(email, org)
    except NoResultFound:
        logger.info(log_prefix + "User(email={}) not found.".format(email))
        user = models.User(org=org, email=email, name=email, group_ids=[])
        models.db.session.add(user)
        models.db.session.commit()

        record_event.delay({
            'org_id': org.id,
            'action': 'create',
            'timestamp': int(time.time()),
            'object_id': user.id,
            'object_type': 'user'
        })
        logger.info(log_prefix + "User(email={}) is created. (id={})".format(email, user.id))

    user.group_ids.append(group.id)
    models.db.session.commit()

    record_event.delay({
        'org_id': org.id,
        'action': 'add_member',
        'timestamp': int(time.time()),
        'object_id': group.id,
        'object_type': 'group',
        'member_id': user.id
    })
    logger.info(log_prefix + "User(id={}, email={}) add to Group(id={}, name={})".format(
        user.id, user.email, group.id, group.name))


def del_member(log_prefix, org, email, group):
    from redash.tasks import record_event

    user = models.User.get_by_email_and_org(email, org)
    user.group_ids.remove(group.id)
    models.db.session.commit()

    record_event.delay({
        'org_id': org.id,
        'action': 'remove_member',
        'timestamp': int(time.time()),
        'object_id': group.id,
        'object_type': 'group',
        'member_id': user.id
    })
    logger.info(log_prefix + "User(id={}, email={}) is removed from Group(id={}, name={})".format(
        user.id, user.email, group.id, group.name))

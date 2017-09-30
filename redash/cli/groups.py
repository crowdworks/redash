from sys import exit

from sqlalchemy.orm.exc import NoResultFound
from flask.cli import AppGroup
from click import argument, option

from redash import models

manager = AppGroup(help="Groups management commands.")


@manager.command()
@argument('name')
@option('--org', 'organization', default='default',
        help="The organization the user belongs to (leave blank for "
        "'default').")
@option('--permissions', default=None,
        help="Comma separated list of permissions ('create_dashboard',"
        " 'create_query', 'edit_dashboard', 'edit_query', "
        "'view_query', 'view_source', 'execute_query', 'list_users',"
        " 'schedule_query', 'list_dashboards', 'list_alerts',"
        " 'list_data_sources') (leave blank for default).")
def create(name, permissions=None, organization='default'):
    print "Creating group (%s)..." % (name)

    org = models.Organization.get_by_slug(organization)

    permissions = extract_permissions_string(permissions)

    print "permissions: [%s]" % ",".join(permissions)

    try:
        models.db.session.add(models.Group(
            name=name, org=org,
            permissions=permissions))
        models.db.session.commit()
    except Exception, e:
        print "Failed create group: %s" % e.message
        exit(1)


@manager.command()
@argument('group_id')
@option('--permissions', default=None,
        help="Comma separated list of permissions ('create_dashboard',"
        " 'create_query', 'edit_dashboard', 'edit_query',"
        " 'view_query', 'view_source', 'execute_query', 'list_users',"
        " 'schedule_query', 'list_dashboards', 'list_alerts',"
        " 'list_data_sources') (leave blank for default).")
def change_permissions(group_id, permissions=None):
    print "Change permissions of group %s ..." % group_id

    try:
        group = models.Group.query.get(group_id)
    except NoResultFound:
        print "User [%s] not found." % group_id
        exit(1)

    permissions = extract_permissions_string(permissions)
    print "current permissions [%s] will be modify to [%s]" % (
        ",".join(group.permissions), ",".join(permissions))

    group.permissions = permissions

    try:
        models.db.session.add(group)
        models.db.session.commit()
    except Exception, e:
        print "Failed change permission: %s" % e.message
        exit(1)


def extract_permissions_string(permissions):
    if permissions is None:
        permissions = models.Group.DEFAULT_PERMISSIONS
    else:
        permissions = permissions.split(',')
        permissions = [p.strip() for p in permissions]
    return permissions


@manager.command()
@option('--org', 'organization', default=None,
        help="The organization to limit to (leave blank for all).")
def list(organization=None):
    """List all groups"""
    if organization:
        org = models.Organization.get_by_slug(organization)
        groups = models.Group.query.filter(models.Group.org == org)
    else:
        groups = models.Group.query

    for i, group in enumerate(groups):
        if i > 0:
            print "-" * 20

        print "Id: {}\nName: {}\nType: {}\nOrganization: {}\nPermission: {}".format(
            group.id, group.name, group.type, group.org.slug, ",".join(group.permissions))

# crowdworks-extended
from redash import settings
import oauth2client
import httplib2
from oauth2client.client import OAuth2Credentials
from apiclient.discovery import build
from sqlalchemy.orm.exc import NoResultFound

@manager.command()
@option('--org', 'organization', default=None,
        help="Sync memberships with Google Groups")
def sync_memberships(organization=None):
    """Sync memberships with Google Groups"""
    if not settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_ENABLED:
        print "GOOGLE_ACCOUNT_CONNECT_OAUTH_ENABLED: {}".format(settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_ENABLED)
        return

    if organization:
        org = models.Organization.get_by_slug(organization)
        groups = models.Group.query.filter(models.Group.org == org)
    else:
        org = models.Organization.get_by_slug('default')
        groups = models.Group.query

    directory_service = google_directory_service()
    first = True

    for i, group in enumerate(groups):
        if '@' not in group.name:
            continue

        if not first:
            print "-" * 20
        first = False

        domains = group.org.google_apps_domains
        google_group_members = get_members(directory_service, group.name, domains)
        exists_members = set([m.email for m in models.Group.members(group.id)])

        print "Id: {}".format(group.id)
        print "Name: {}".format(group.name)
        print "Type: {}".format(group.type)
        print "Organization: {}".format(group.org.slug)
        print "GoogleAppDomains: {}".format(', '.join(domains))
        print "ExistsMembers: ({}) {}".format(len(exists_members), ', '.join(exists_members))
        print "GoogleGroupMembers: ({}) {}".format(len(google_group_members), ', '.join(google_group_members))

        if exists_members == google_group_members:
            print "* already synced."
        else:
            print "* sync start ..."

        for add_email in (google_group_members - exists_members):
            add_member(org, add_email, group.id)

        for del_email in (exists_members - google_group_members):
            del_member(org, del_email, group.id)


def google_directory_service():
    credential = OAuth2Credentials.from_json(settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_TOKEN)
    http_auth = credential.authorize(httplib2.Http())
    credential.refresh(http_auth)
    service = build('admin', 'directory_v1', http=http_auth)
    return service


def get_members(service, group_key, domains):
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
                members.extend(get_members(service, member['email'], domains))
        if 'nextPageToken' in result:
            pageToken = result['nextPageToken']
        else:
            break
    return set(members)


def add_member(org, email, group_id):
    try:
        user = models.User.get_by_email_and_org(email, org)
    except NoResultFound:
        print "[add] {} ... NotFound".format(email)
        user = models.User(org=org, email=email, name=email, group_ids=[])
        models.db.session.add(user)
        models.db.session.commit()
        print "[add] {} ... User Created.".format(email)

    group = models.Group.get_by_id_and_org(group_id, org)
    user.group_ids.append(group.id)
    models.db.session.commit()

    print "[add] {} DONE".format(email)


def del_member(org, email, group_id):
    user = models.User.get_by_email_and_org(email, org)
    user.group_ids.remove(group_id)
    models.db.session.commit()

    print "[del] {} DONE".format(email)

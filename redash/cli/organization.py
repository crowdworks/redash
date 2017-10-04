from click import argument
from flask.cli import AppGroup

from redash import models

manager = AppGroup(help="Organization management commands.")


@manager.command()
@argument('domains')
def set_google_apps_domains(domains):
    """
    Sets the allowable domains to the comma separated list DOMAINS.
    """
    organization = models.Organization.query.first()
    k = models.Organization.SETTING_GOOGLE_APPS_DOMAINS
    organization.settings[k] = domains.split(',')
    models.db.session.add(organization)
    models.db.session.commit()
    print "Updated list of allowed domains to: {}".format(
        organization.google_apps_domains)


@manager.command()
def show_google_apps_domains():
    organization = models.Organization.query.first()
    print "Current list of Google Apps domains: {}".format(
        ', '.join(organization.google_apps_domains))


@manager.command()
def list():
    """List all organizations"""
    google_setting_key = models.Organization.SETTING_GOOGLE_APPS_DOMAINS
    orgs = models.Organization.query
    for i, org in enumerate(orgs):
        if i > 0:
            print "-" * 20

        print "Id: {}\nName: {}\nSlug: {}".format(org.id, org.name, org.slug)
        if org.settings and google_setting_key in org.settings:
            domains = ", ".join(org.settings[google_setting_key])
            print "Domains: {}".format(domains)


# crowdworks-extended
from click import option

@manager.command()
@argument('name')
@argument('slug')
@option('--domains', default=None, help="Set allowable domains to comma separated list DOMAINS.")
def create(name, slug, domains=None):
    try:
        print "Create organization ({} slug={}) ...".format(name, slug)
    except UnicodeDecodeError, e:
        print "Create organization (slug={})".format(slug)

    if domains:
        domains = domains.split(',')
        domains = [d.strip() for d in domains]
        print "domains: [%s]" % ",".join(domains)
    else:
        print "domains: None"

    try:
        org = models.Organization(name=name, slug=slug)
        org.settings = {
            models.Organization.SETTING_IS_PUBLIC: True
        }
        if domains:
            k = models.Organization.SETTING_GOOGLE_APPS_DOMAINS
            org.settings[k] = domains
        models.db.session.add(org)

        admin_group = models.Group(org=org, type=models.Group.BUILTIN_GROUP, name="admin", permissions=[])
        default_group = models.Group(org=org, type=models.Group.BUILTIN_GROUP, name="default", permissions=[])
        models.db.session.add(admin_group)
        models.db.session.add(default_group)

        models.db.session.commit()
        print "done."
    except Exception, e:
        print "Failed create organization: %s" % e.message
        exit(1)

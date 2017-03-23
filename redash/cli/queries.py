from sys import exit

import json
import cStringIO
import xlsxwriter

from click import BOOL, argument, option, prompt
from flask.cli import AppGroup

from redash import models

EXPORT_FILE_PREFIX = "redash_results"
manager = AppGroup(help="Queries management commands.")


def query_report(query):
    status = "published"
    if query.is_archived:
        status = "archived"
    elif query.is_draft:
        status = "draft"

    print "Id: {}".format(query.id)
    print "Name: {}".format(query.name)
    print "Description: {}".format(query.description)
    print "Status: {}".format(status)
    print "Organization: {}".format(query.org.name)
    print "Schedule: {}".format(query.schedule)
    print "ScheduleFailures: {}".format(query.schedule_failures)
    print "LatestQueryDataId: {}".format(query.latest_query_data_id)


def query_result_export(query_result, filename):
    s = cStringIO.StringIO()

    query_data = json.loads(query_result.data)
    book = xlsxwriter.Workbook(s)
    sheet = book.add_worksheet("result")

    column_names = []
    for (c, col) in enumerate(query_data['columns']):
        sheet.write(0, c, col['name'])
        column_names.append(col['name'])

    for (r, row) in enumerate(query_data['rows']):
        for (c, name) in enumerate(column_names):
            sheet.write(r + 1, c, row.get(name))

    book.close()

    with open(filename, 'w') as f:
        f.write(s.getvalue())


def get_queries(organization):
    if organization:
        org = models.Organization.get_by_slug(organization)
        queries = models.Query.query.filter(models.Query.org == org)
    else:
        queries = models.Query.query

    return queries


@manager.command()
@option('--org', 'organization', default=None,
        help="The organization the user belongs to (leave blank for all"
        " organizations)")
def list(organization=None):
    queries = get_queries(organization)
    for i, query in enumerate(queries):
        if i > 0:
            print "-" * 20

        query_report(query)


@manager.command()
@argument('query_id')
@option('--org', 'organization', default=None,
        help="The organization the user belongs to (leave blank for all"
        " organizations)")
def get(query_id, organization=None):
    """List all users"""
    if organization:
        org = models.Organization.get_by_slug(organization)
        query = models.Query.get_by_id_and_org(query_id, organization)
    else:
        query = models.Query.get_by_id(query_id)

    query_report(query)


@manager.command()
@option('--org', 'organization', default=None,
        help="The organization the user belongs to (leave blank for all"
        " organizations)")
def export(organization=None):
    queries = get_queries(organization)
    for i, query in enumerate(queries):
        if query.is_draft or query.is_archived:
            print "QueryID: {} is draft or archived. skip.".format(query.id)
            continue

        print "QueryID: {} is exporting ...".format(query.id)
        query_result = query.latest_query_data
        filename = "export/{}_{}.xlsx".format(EXPORT_FILE_PREFIX, query.id)
        query_result_export(query_result, filename)
        print "QueryID: {} is exporting ... {} done.".format(query.id, filename)

# coding: utf-8
# crowdworks-extended

from celery.utils.log import get_task_logger
from redash.worker import celery
from redash import models, settings

logger = get_task_logger(__name__)

import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.client import OAuth2Credentials
import json
import datetime
import os
from pytz import timezone


def sheet_name(query_id, executed_at=None):
    if executed_at:
        return "{name}:id={query_id};executed={executed_at}".format(
            name=settings.NAME, query_id=query_id, executed_at=executed_at
        )
    else:
        return "{name}:id={query_id};".format(name=settings.NAME, query_id=query_id)


@celery.task(name="redash.tasks.export_google_spreadsheet")
def export_google_spreadsheet(query_id):
    if not settings.EXPORT_GOOGLE_SPREADSHEET_ENABLED:
        logger.info("Export to Google Spreadsheet is disabled.")
        return

    query = models.Query.query.get(query_id)
    if not (query.options and 'spreadsheetUrl' in query.options and query.options['spreadsheetUrl']):
        logger.info("query_id={} does not have settings for export.".format(query_id))
        return

    query_result = models.QueryResult.get_latest(query.data_source, query.query_text, max_age=-1)
    if query_result is None:
        logger.info("query_id={} (data_source={}, query_hash={}) QueryResult not found".format(
            query_id, query.data_source_id, query.query_hash))
        return

    try:
        _update_spreadsheet_data(query_id, query, query_result)
        spreadsheet_update_slack_notify(status="success", query=query)
    except Exception as e:
        error = unicode(e)
        logger.warning('Unexpected error while _update_spreadsheet_data: {}'.format(error), exc_info=1)
        spreadsheet_update_slack_notify(status="failed", query=query, exception=e)
        raise e


def _update_spreadsheet_data(query_id, query, query_result):

    query_data = json.loads(query_result.data)
    columns = query_data['columns']
    rows = query_data['rows']
    payload = []
    for col in columns:
        payload.append(col['friendly_name'])
    for row in rows:
        for col in columns:
            payload.append(row[col['name']])

    addr = rowcol_to_a1(len(rows) + 1, len(columns))
    range_addr = "A1:{}".format(addr)

    spreadsheet_url = query.options['spreadsheetUrl']
    logger.info("query_id={} export sheet URL: {}".format(query_id, spreadsheet_url))
    spreadsheet = _get_spreadsheet_service().open_by_url(spreadsheet_url)

    worksheet = None
    for ws in spreadsheet.worksheets():
        if worksheet:
            break
        if ws.title.startswith(sheet_name(query_id)):
            worksheet = ws

    if worksheet:
        logger.info("query_id={} (worksheet detect: {})".format(query_id, worksheet))
    else:
        worksheet = spreadsheet.add_worksheet(title=sheet_name(query_id), rows="100", cols="20")
        logger.info("query_id={} (worksheet created. {})".format(query_id, worksheet))

    offset_columns = len(columns) + 1
    num_rows = len(rows)

    worksheet.resize(1,1)

    worksheet.resize(num_rows + 1, offset_columns)
    cell_list = worksheet.range(range_addr)
    for i, cell in enumerate(cell_list):
        cell.value = payload[i]
    worksheet.update_cells(cell_list)

    _update_spreadsheet_metadata(
        worksheet=worksheet,
        query=query,
        query_result=query_result,
        offset_columns=offset_columns,
        num_rows=num_rows
    )


def _update_spreadsheet_metadata(worksheet, query, query_result, offset_columns, num_rows):
    if settings.MULTI_ORG:
        org_slug = query.data_source.org.slug
        query_url = 'https://{host}/{org_slug}/queries/{query_id}'.format(host=settings.HOST, org_slug=org_slug, query_id=query.id)
    else:
        query_url = 'https://{host}/queries/{query_id}'.format(host=settings.HOST, query_id=query.id)

    metadata = [
        {
            'title': 'Query executed_at',
            'value': query_result.retrieved_at.astimezone(timezone(os.environ.get('TZ', 'UTC'))).strftime('%Y/%m/%d %H:%M'),
        },
        {
            'title': 'Export executed_at',
            'value': datetime.datetime.now(tz=timezone(os.environ.get('TZ', 'UTC'))).strftime('%Y/%m/%d %H:%M'),
        },
        {
            'title': 'Source',
            'value': query_url
        },
    ]

    metadata_payload = []
    for m in metadata:
        metadata_payload.append(m['title'])
    for m in metadata:
        metadata_payload.append(m['value'])

    if num_rows == 0:
        worksheet.resize(2, offset_columns + len(metadata))
    else:
        worksheet.resize(num_rows + 1, offset_columns + len(metadata))

    metadata_range = "{}:{}".format(
        rowcol_to_a1(1, offset_columns + 1),
        rowcol_to_a1(2, offset_columns + len(metadata))
    )
    metadata_cells = worksheet.range(metadata_range)

    for i, m in enumerate(metadata_payload):
        metadata_cells[i].value = m
    worksheet.update_cells(metadata_cells)


def _get_spreadsheet_service():
    credentials = OAuth2Credentials.from_json(settings.EXPORT_GOOGLE_SPREADSHEET_OAUTH_TOKEN)
    return gspread.authorize(credentials)


import requests
def spreadsheet_update_slack_notify(status, query, exception=None):
    if not settings.QUERY_ERROR_REPORT_ENABLED:
        return

    host = settings.HOST
    url = settings.QUERY_ERROR_REPORT_SLACK_WEBHOOK_URL
    channel = settings.QUERY_ERROR_REPORT_SLACK_CHANNEL
    username = settings.QUERY_ERROR_REPORT_SLACK_USERNAME
    icon_emoji = settings.QUERY_ERROR_REPORT_SLACK_ICON_EMOJI

    if query and query.id:
        if settings.MULTI_ORG:
            org_slug = query.org.slug
            query_link = "{host}/{org_slug}/queries/{query_id}".format(host=host, org_slug=org_slug, query_id=query.id)
        else:
            query_link = "{host}/queries/{query_id}".format(host=host, query_id=query.id)
    else:
        if settings.MULTI_ORG:
            org_slug = query.org.slug
            query_link = "{host}/{org_slug}/ (adhoc query)".format(host=host, org_slug=org_slug)
        else:
            query_link = "{host} (adhoc query)".format(host=host)

    user = query.user

    attachments = []

    if status == "success":
        title = "QueryResult exports success"
        color = "#439fe0"
    else:
        title = "QueryResult exports failed"
        color = "#c0392b"

    attachments.append({
        "text": title,
        "mrkdwn_in": ["text"],
        "color": color,
        "fields": [
            {
                "title": "User",
                "value": user.email if user is not None else "(unknown)",
                "short": True,
            },
            {
                "title": "Query Link",
                "value": query_link,
                "short": True,
            },
            {
                "title": "Query Title",
                "value": query.name,
                "short": False,
            },
            {
                "title": "Spreadsheet URL",
                "value": query.options['spreadsheetUrl'],
                "short": False,
            },
        ]
    })

    attachments.append({
        "title": "Query",
        "text": "```\n" + query.query_text.strip() + "\n```",
        "mrkdwn_in": ["text"],
        "color": color,
    })

    if exception:
        err_message = unicode(exception.__class__)
        if len(unicode(exception.message).strip()) > 0:
            err_message += "\n```\n{}```".format(unicode(exception.message).strip())

        attachments.append({
            "title": "Error Message",
            "text": err_message,
            "mrkdwn_in": ["text"],
            "color": color,
        })

    payload = {'attachments': attachments}

    if username: payload['username'] = username
    if icon_emoji: payload['icon_emoji'] = icon_emoji
    if channel: payload['channel'] = channel

    try:
        resp = requests.post(url, data=json.dumps(payload))
        logger.warning(resp.text)
        if resp.status_code == 200:
            logger.info("Slack send Success. status_code => {status}".format(status=resp.status_code))
        else:
            logger.error("Slack send ERROR. status_code => {status}".format(status=resp.status_code))

    except Exception:
        logger.exception("Slack send ERROR.")

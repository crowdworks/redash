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


@celery.task(name="redash.tasks.export_google_spreadsheet")
def export_google_spreadsheet(query_id):
    if not settings.EXPORT_GOOGLE_SPREADSHEET_ENABLED:
        logger.info("Export to Google Spreadsheet is disabled.")
        return

    query = models.Query.query.get(query_id)
    if not (query.options and query.options['spreadsheetUrl']):
        logger.info("query_id={} does not have settings for export.".format(query_id))
        return

    query_result = models.QueryResult.get_latest(query.data_source, query.query_text, max_age=-1)
    if query_result is None:
        logger.info("query_id={} (data_source={}, query_hash={}) QueryResult not found".format(
            query_id, query.data_source_id, query.query_hash))
        return

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

    worksheet_id = None
    if '#gid=' in spreadsheet_url:
        worksheet_id = spreadsheet_url.split('#', 2)[1].replace('gid=', '')
        logger.info("query_id={} (worksheet_id={})".format(query_id, worksheet_id))

    worksheet = None
    for ws in spreadsheet.worksheets():
        if worksheet:
            break
        if ws.id == worksheet_id:
            worksheet = ws
    for ws in spreadsheet.worksheets():
        if worksheet:
            break
        if ws.title.startswith("redash: query={}".format(query_id)):
            worksheet = ws

    if worksheet:
        logger.info("query_id={} (worksheet detect: {})".format(query_id, worksheet))
    else:
        worksheet = spreadsheet.add_worksheet(title="redash: query={}".format(query_id), rows="100", cols="20")
        logger.info("query_id={} (worksheet created. {})".format(query_id, worksheet))

    worksheet.resize(len(rows) + 1, len(columns))
    cell_list = worksheet.range(range_addr)
    for i, cell in enumerate(cell_list):
        cell.value = payload[i]
    worksheet.update_cells(cell_list)
    worksheet.update_title("redash: query={} execute={}".format(
        query_id,
        query_result.retrieved_at.astimezone(timezone(os.environ.get('TZ', 'UTC'))).strftime('%Y/%m/%d %H:%M')
    ))


def _get_spreadsheet_service():
    credentials = OAuth2Credentials.from_json(settings.EXPORT_GOOGLE_SPREADSHEET_OAUTH_TOKEN)
    return gspread.authorize(credentials)

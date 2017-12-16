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


def cell_name(query_id, executed_at=None):
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
        if ws.title.startswith(cell_name(query_id)):
            worksheet = ws

    if worksheet:
        logger.info("query_id={} (worksheet detect: {})".format(query_id, worksheet))
    else:
        worksheet = spreadsheet.add_worksheet(title=cell_name(query_id), rows="100", cols="20")
        logger.info("query_id={} (worksheet created. {})".format(query_id, worksheet))

    worksheet.resize(len(rows) + 1, len(columns) + 3)
    cell_list = worksheet.range(range_addr)
    for i, cell in enumerate(cell_list):
        cell.value = payload[i]
    worksheet.update_cells(cell_list)

    if len(rows) == 0:
        worksheet.resize(2, len(columns) + 3)

    metadata_range = "{}:{}".format(
        rowcol_to_a1(1, len(columns) + 2),
        rowcol_to_a1(2, len(columns) + 3)
    )
    metadata_cells = worksheet.range(metadata_range)
    metadata_cells[0].value = 'Query executed_at'
    metadata_cells[1].value = 'Export executed_at'
    metadata_cells[2].value = query_result.retrieved_at.astimezone(timezone(os.environ.get('TZ', 'UTC'))).strftime('%Y/%m/%d %H:%M')
    metadata_cells[3].value = datetime.datetime.now(tz=timezone(os.environ.get('TZ', 'UTC'))).strftime('%Y/%m/%d %H:%M')
    worksheet.update_cells(metadata_cells)


def _get_spreadsheet_service():
    credentials = OAuth2Credentials.from_json(settings.EXPORT_GOOGLE_SPREADSHEET_OAUTH_TOKEN)
    return gspread.authorize(credentials)

# coding: utf-8
# crowdworks-extended

import logging
import base64
import gzip
from StringIO import StringIO
from redash import settings
from redash.permissions import require_admin
from redash.handlers.base import BaseResource, routes

from oauth2client.client import OAuth2WebServerFlow, FlowExchangeError
from flask import request, url_for, redirect, render_template
from flask_restful import abort


def get_oauth_flow():
    scopes = [
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile',
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/admin.directory.group.member.readonly',
        'https://www.googleapis.com/auth/admin.directory.group.readonly'
    ]
    flow = OAuth2WebServerFlow(
        client_id=settings.GOOGLE_ACCOUNT_CONNECT_CLIENT_ID,
        client_secret=settings.GOOGLE_ACCOUNT_CONNECT_CLIENT_SECRET,
        scope=scopes,
        redirect_uri=url_for('redash.datasource_google_connected_callback', _external=True),
        access_type='offline',
        prompt='consent'
    )
    return flow


@require_admin
@routes.route('/google_account/connect', methods=['GET'], endpoint='datasource_google_connect')
def data_source_google_oauth():
    if not settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_SERVER_ENABLED:
        logging.info("data_source google_oauth skipped.")
        abort(400)

    flow = get_oauth_flow()

    return redirect(flow.step1_get_authorize_url())


@require_admin
@routes.route('/google_account/callback', methods=['GET'], endpoint='datasource_google_connected_callback')
def data_source_google_oauth_callback():
    if not settings.GOOGLE_ACCOUNT_CONNECT_OAUTH_SERVER_ENABLED:
        logging.info("data_source google_oauth skipped.")
        abort(400)

    code = request.args.get('code', None)
    if not code:
        return redirect(url_for('redash.datasource_google_connect'))

    flow = get_oauth_flow()

    try:
        credentials = flow.step2_exchange(code)
    except FlowExchangeError as e:
        logging.warn('except Error:', e)
        return redirect(url_for('redash.datasource_google_connect'))

    credential_json = credentials.to_json()
    email = credentials.id_token['email']

    io = StringIO()
    with gzip.GzipFile(fileobj=io, mode='wb') as f:
        f.write(credential_json)
    token = base64.b64encode(io.getvalue())

    text = "REDASH_GOOGLE_ACCOUNT_CONNECT_EMAIL={email}\nREDASH_GOOGLE_ACCOUNT_CONNECT_OAUTH_TOKEN={token}".format(
        email=email,
        token=token
    )
    return render_template("google_account_connected.html", text=text)

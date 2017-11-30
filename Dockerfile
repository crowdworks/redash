FROM redash/base:latest

RUN set -ex && \
    apt-get update && \
    apt-get install -y python-pyasn1 python-pyasn1-modules && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# We first copy only the requirements file, to avoid rebuilding on every file
# change.
COPY requirements.txt requirements_dev.txt requirements_all_ds.txt requirements_crowdworks.txt ./
RUN pip install -r requirements.txt -r requirements_dev.txt -r requirements_all_ds.txt -r requirements_crowdworks.txt

COPY . ./
RUN npm install && npm run build && rm -rf node_modules
RUN chown -R redash /app
USER redash

ENTRYPOINT ["/app/bin/docker-entrypoint"]

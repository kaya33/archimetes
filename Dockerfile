FROM python:2.7

RUN mkdir -p /usr/src/archimedes
WORKDIR /usr/src/archimedes

ONBUILD COPY requirements.txt /usr/src/archimedes/
ONBUILD RUN pip install --no-cache-dir -r requirements.txt

ONBUILD COPY . /usr/src/archimedes

COPY docker-entrypoint.sh /usr/local/bin/
RUN ln -s usr/local/bin/docker-entrypoint.sh /entrypoint.sh # backwards compat
ENTRYPOINT ["docker-entrypoint.sh"]


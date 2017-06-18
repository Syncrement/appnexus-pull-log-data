# NOTE: This was not tested yet, but should be a way to install this service using Docker. Still need to
# figure out how to pass in parameters i.e. custom pulllogleveldata-config file.
FROM python:3

ENV APP_DIR /pull-loglevel-data
ENV SCRIPT_NAME pulllogleveldata.py

WORKDIR ${APP_DIR}


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ADD ./${SCRIPT_NAME} ./${APP_DIR}

CMD [ "python", "./${SCRIPT_NAME}" ]
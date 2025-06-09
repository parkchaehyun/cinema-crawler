# ────────────────────────────────────────────────────────
# Stage 1: chrome-builder (only builds Chromium binaries)
# ────────────────────────────────────────────────────────
FROM public.ecr.aws/lambda/python:3.11 AS chrome-builder

RUN yum install -y -q unzip curl sudo
ENV CHROMIUM_VERSION=1002910

COPY install-browser.sh /tmp/
RUN /usr/bin/bash /tmp/install-browser.sh

# ────────────────────────────────────────────────────────
# Stage 2: crawler (Chrome + Selenium)
# ────────────────────────────────────────────────────────
FROM public.ecr.aws/lambda/python:3.11 AS crawler

# Chromium dependencies
COPY chrome-deps.txt /tmp/
RUN yum install -y $(cat /tmp/chrome-deps.txt) && yum clean all

# Copy Chrome + Chromedriver from chrome-builder
COPY --from=chrome-builder /opt/chrome /opt/chrome
COPY --from=chrome-builder /opt/chromedriver /opt/chromedriver

# App code and deps
COPY requirements-crawler.txt .
COPY crawlers/ /var/task/crawlers/
COPY models.py /var/task/models.py

RUN pip install --upgrade pip && pip install -r requirements-crawler.txt

ENV CHROME_BIN=/opt/chrome/chrome
ENV CHROMEDRIVER_PATH=/opt/chromedriver

WORKDIR /var/task
CMD ["crawlers.lambda_function.lambda_handler"]

# ────────────────────────────────────────────────────────
# Stage 3: tmdb (lightweight, no Chrome needed)
# ────────────────────────────────────────────────────────
FROM public.ecr.aws/lambda/python:3.11 AS tmdb

WORKDIR /var/task

COPY requirements-tmdb.txt .
RUN pip install --upgrade pip && pip install -r requirements-tmdb.txt -t .

COPY crawlers/poster_updater.py lambda_function.py
COPY crawlers/supabase_client.py supabase_client.py
COPY models.py models.py

CMD ["lambda_function.lambda_handler"]

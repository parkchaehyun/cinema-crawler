FROM public.ecr.aws/lambda/python:3.11 AS stage

# Install basic tools
RUN yum install -y -q unzip curl sudo

# Set Chromium version
ENV CHROMIUM_VERSION=1002910

# Add install script
COPY install-browser.sh /tmp/
RUN /usr/bin/bash /tmp/install-browser.sh

FROM public.ecr.aws/lambda/python:3.11

# Install dependencies for Chromium
COPY chrome-deps.txt /tmp/
RUN yum install -y $(cat /tmp/chrome-deps.txt) && yum clean all

# Copy browser binaries from builder stage
COPY --from=stage /opt/chrome /opt/chrome
COPY --from=stage /opt/chromedriver /opt/chromedriver

# Copy your Python code
COPY requirements.txt .
COPY crawlers/ /var/task/crawlers/
COPY models.py /var/task/models.py

# Install dependencies into Lambda's root
RUN pip install --upgrade pip && pip install -r requirements.txt

# Set environment vars for Selenium to find binaries
ENV CHROME_BIN=/opt/chrome/chrome
ENV CHROMEDRIVER_PATH=/opt/chromedriver

WORKDIR /var/task
CMD ["crawlers.lambda_function.lambda_handler"]

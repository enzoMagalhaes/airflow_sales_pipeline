# THIS DOCKERFILE IS ONLY USED IN DEVELOPMENT TO INSTALL PYTEST

FROM apache/airflow:2.3.0
COPY test_requirements.txt /test_requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /test_requirements.txt

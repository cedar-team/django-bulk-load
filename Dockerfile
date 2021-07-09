FROM python:3.6.8
WORKDIR /python
COPY . ./
RUN pip install -e .[test]
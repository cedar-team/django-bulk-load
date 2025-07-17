FROM python:3.9
WORKDIR /python
COPY . ./
RUN pip install -e .[test]
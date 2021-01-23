FROM python:3.8-alpine

# install the C build tools required for the Numpy's libraries
RUN apk add build-base

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
RUN python -m pip install --upgrade pip

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install requests

# copy project's source code
COPY . .
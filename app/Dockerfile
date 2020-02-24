FROM python:3.7.6-stretch
USER root
RUN wget -q https://www.postgresql.org/media/keys/ACCC4CF8.asc -O - | apt-key add -
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" >> /etc/apt/sources.list.d/pgdg.list'
RUN apt-get update -y
RUN apt-get install postgresql postgresql-contrib -y
ADD . /app
WORKDIR /app
RUN pip3 install -r requirements.txt
CMD python3 pg_loader.py
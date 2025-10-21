FROM ubuntu:noble AS base
LABEL authors="sandyr"

ENV DEBIAN_FRONTEND="noninteractive" TZ="Etc/UTC"
RUN apt -y update && \
    apt install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt -y upgrade && \
    apt -y install \
        libpq-dev \
        python3.12-dev \
        python3.12-venv \
        gcc \
        python-is-python3 \
        tzdata \
        git \
        build-essential \
        python3-pip \
        python3-setuptools \
        python3-wheel

WORKDIR /app
COPY requirements.txt .
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --use-pep517 --upgrade -r requirements.txt

FROM base AS django
COPY requirements* .
RUN pip install --ignore-installed --use-pep517 -r requirements-dev.txt
RUN pip install --ignore-installed --use-pep517 -r requirements-tools.txt
RUN pip --no-input uninstall pydantic; pip --no-input install pydantic==2.10.6
#TODO: remove the above once https://github.com/eadwinCode/django-ninja-jwt/pull/151
COPY . .
RUN python manage.py collectstatic --noinput

FROM django AS agent

RUN groupadd -r slurm
RUN useradd -rm -d /home/slurm -s /bin/bash -g slurm -G sudo -u 999 slurm
RUN mkdir -p /home/slurm && chown -R slurm:slurm /home/slurm
RUN groupadd -r -g 555 munge
RUN useradd -rms /bin/bash -g munge -u 555 munge

RUN apt -y update && apt -y upgrade
RUN apt-get install -y \
    curl \
    libmunge-dev \
    munge \
    gosu \
    netcat-traditional \
    libcurl4-openssl-dev \
    libhttp-parser-dev \
    libjson-c-dev \
    libyaml-dev \
    libjwt-dev \
    libmariadb-dev \
    libmariadb-dev-compat \
    lua5.4 \
    dbus \
    cron \
    sudo \
    cpio

RUN curl -sL https://download.schedmd.com/slurm/slurm-24.11.6.tar.bz2 | tar -xj
RUN cd slurm-24.11.6 && \
    ./configure --sysconfdir=/etc/slurm && \
    make install -j16

COPY slurm-dev-environment/configs/slurm.conf /etc/slurm/slurm.conf
RUN chown -R slurm:slurm /etc/slurm/
RUN mkdir -p /run/munge && chown -R munge /run/munge
COPY slurm-dev-environment/entrypoints/submitter-entrypoint.sh /usr/local/bin/submitter-entrypoint.sh
RUN chmod +x /usr/local/bin/submitter-entrypoint.sh
COPY slurm-dev-environment/entrypoints/prefect-slurm-entrypoint.sh /usr/local/bin/prefect-slurm-entrypoint.sh
RUN chmod +x /usr/local/bin/prefect-slurm-entrypoint.sh

ENV SLURM_LIB_DIR=/usr/local/lib
ENV SLURM_INCLUDE_DIR=/usr/local/include
RUN pip install --upgrade pip setuptools wheel
RUN pip install --ignore-installed --upgrade --use-pep517 --no-build-isolation https://github.com/PySlurm/pyslurm/archive/refs/tags/v24.11.0.tar.gz
ENV TZ="Etc/UTC"

ENTRYPOINT ["/usr/local/bin/submitter-entrypoint.sh", "python", "manage.py"]

FROM python:3.12 AS ci
WORKDIR /app
COPY requirements* .
RUN pip install --ignore-installed --use-pep517 -r requirements.txt
RUN pip install --ignore-installed --use-pep517 -r requirements-dev.txt
RUN pip install --ignore-installed --use-pep517 -r requirements-tools.txt
RUN pip --no-input uninstall pydantic; pip --no-input install pydantic==2.10.6
#TODO: remove the above once https://github.com/eadwinCode/django-ninja-jwt/pull/151
COPY . .
RUN python manage.py collectstatic --noinput

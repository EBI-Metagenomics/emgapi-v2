FROM ubuntu:jammy AS base
LABEL authors="sandyr"

ENV DEBIAN_FRONTEND="noninteractive" TZ="Etc/UTC"
RUN apt -y update && apt -y upgrade && apt -y install libpq-dev python3-pip python-is-python3 tzdata git

WORKDIR /app
COPY requirements.txt .
RUN pip install --upgrade pip setuptools wheel
RUN pip install --upgrade -r requirements.txt

FROM base AS django
COPY . .
RUN pip install --upgrade -r requirements-dev.txt
RUN pip install --upgrade -r requirements-tools.txt
ENTRYPOINT ["python3", "manage.py"]

FROM base AS agent
RUN apt -y update && apt -y upgrade
RUN apt -y install munge gosu netcat-traditional slurm-wlm libslurm-dev
COPY slurm-dev-environment/configs/slurm_single_node.conf /etc/slurm/slurm.conf
RUN chown -R slurm:slurm /etc/slurm/
RUN mkdir -p /run/munge && chown -R munge /run/munge
COPY slurm-dev-environment/entrypoints/submitter-entrypoint.sh /usr/local/bin/submitter-entrypoint.sh
RUN chmod +x /usr/local/bin/submitter-entrypoint.sh

# Handle apple silicon and x86-64
RUN mkdir -p /slurm
RUN if [ "$(uname -m)" = "x86_64" ]; then \
        ln -s /usr/lib/x86_64-linux-gnu /slurm/lib; \
    elif [ "$(uname -m)" = "aarch64" ]; then \
        ln -s /usr/lib/aarch64-linux-gnu /slurm/lib; \
    else \
        exit 1; \
    fi

ENV SLURM_LIB_DIR=/slurm/lib
ENV SLURM_INCLUDE_DIR=/usr/include
RUN pip install https://github.com/PySlurm/pyslurm/archive/refs/tags/v21.8.1.tar.gz
ENV TZ="Etc/UTC"

COPY . .
RUN pip install --upgrade -r requirements-dev.txt
RUN pip install --upgrade -r requirements-tools.txt

ENTRYPOINT ["/usr/local/bin/submitter-entrypoint.sh", "python3", "manage.py"]

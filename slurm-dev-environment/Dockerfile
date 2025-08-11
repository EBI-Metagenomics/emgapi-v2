FROM ubuntu:jammy AS base
LABEL authors="mgnify team"

RUN groupadd -r slurm
RUN useradd -rm -d /home/slurm -s /bin/bash -g slurm -G sudo slurm 
RUN mkdir -p /home/slurm && chown -R slurm:slurm /home/slurm
RUN useradd -rms /bin/bash munge 

RUN apt -y update && apt -y upgrade && apt -y install curl nano wget zip bzip2 rsync build-essential

RUN apt -y install python3-pip python-is-python3

RUN apt -y install sudo gosu netcat-traditional

RUN apt-get install -y \
    libmunge-dev \
    munge \
    libcurl4-openssl-dev \
    libhttp-parser-dev \
    libjson-c-dev \
    libyaml-dev \
    libjwt-dev \
    libmariadb-dev \
    libmariadb-dev-compat \
    lua5.4 \
    dbus

RUN curl -sL https://download.schedmd.com/slurm/slurm-24.11.6.tar.bz2 | tar -xj
RUN cd slurm-24.11.6 && \
    ./configure --sysconfdir=/etc/slurm && \
    make install -j16

COPY configs/slurm.conf /etc/slurm/slurm.conf
COPY configs/slurmdbd.conf /etc/slurm/slurmdbd.conf
COPY configs/cgroup.conf /etc/slurm/cgroup.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

RUN mkdir -p /run/munge && chown -R munge /run/munge
RUN mkdir -p /var/run/slurm && chown -R slurm:slurm /var/run/slurm
RUN mkdir -p /var/log/slurm && chown -R slurm:slurm /var/log/slurm
RUN mkdir -p /var/spool/slurmctld && chown -R slurm:slurm /var/spool/slurmctld
RUN mkdir -p /var/spool/slurmd && chown -R slurm:slurm /var/spool/slurmd

# Setup jwt key
COPY configs/jwt_hs256.key /var/spool/slurmctld/jwt_hs256.key
RUN chown slurm:slurm /var/spool/slurmctld/jwt_hs256.key
RUN chmod 0600 /var/spool/slurmctld/jwt_hs256.key

FROM base AS dbd
# Just the database daemon
COPY entrypoints/dbd-entrypoint.sh /usr/local/bin/dbd-entrypoint.sh
RUN chmod +x /usr/local/bin/dbd-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/dbd-entrypoint.sh"]

FROM base AS ctl
# Just the controller
COPY entrypoints/ctl-entrypoint.sh /usr/local/bin/ctl-entrypoint.sh
RUN chmod +x /usr/local/bin/ctl-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/ctl-entrypoint.sh"]

FROM base AS worker
# The slurm worker, and base HPC tools like Nextflow
COPY entrypoints/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
ENV SLURM_INCLUDE_DIR=/usr/include
ENV SLURM_LIB_DIR=/usr/lib/x86_64-linux-gnu
#RUN DEBIAN_FRONTEND=noninteractive \
#    apt -y update && apt install -y default-jre

# Replace the default-jre installation with Java 17
RUN DEBIAN_FRONTEND=noninteractive \
    apt -y update && \
    apt install -y software-properties-common && \
    add-apt-repository -y ppa:openjdk-r/ppa && \
    apt update && \
    apt install -y openjdk-17-jre-headless

RUN wget -qO- https://get.nextflow.io | bash
RUN chmod +x nextflow
RUN mv nextflow /usr/bin/
COPY configs/slurm_prolog.sh /usr/local/bin/slurm_prolog.sh
COPY configs/slurm_epilog.sh /usr/local/bin/slurm_epilog.sh

RUN mkdir -p /usr/bin/webin-cli
RUN wget https://github.com/enasequence/webin-cli/releases/download/7.3.0/webin-cli-7.3.0.jar -O /usr/bin/webin-cli/webin-cli.jar
RUN chmod +x /usr/bin/webin-cli/webin-cli.jar
ENV PATH="/usr/bin/webin-cli:$PATH"

ENTRYPOINT ["/usr/local/bin/worker-entrypoint.sh"]

FROM base AS restd
# Just the rest api daemon
ENV SLURM_JWT=daemon
ENV SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files,disable_user_check
COPY entrypoints/restd-entrypoint.sh /usr/local/bin/restd-entrypoint.sh
RUN chmod +x /usr/local/bin/restd-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/restd-entrypoint.sh"]

FROM worker AS single_node
# DBD + Ctl + Worker in a single container
ENV SLURM_JWT=daemon
ENV SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files,disable_user_check
COPY entrypoints/single-node-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
COPY configs/slurm_single_node.conf /etc/slurm/slurm.conf
COPY configs/slurmdbd_single_node.conf /etc/slurm/slurmdbd.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

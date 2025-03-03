FROM ubuntu:jammy as base
LABEL authors="mgnify team"

RUN apt -y update && apt -y upgrade && apt -y install curl nano wget zip bzip2 rsync

RUN apt -y install munge python3-pip python-is-python3

RUN apt -y install sudo gosu netcat-traditional slurm-wlm

COPY configs/slurm.conf /etc/slurm/slurm.conf
COPY configs/slurmdbd.conf /etc/slurm/slurmdbd.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

RUN mkdir -p /run/munge && chown -R munge /run/munge
RUN mkdir -p /var/run/slurm && chown -R slurm:slurm /var/run/slurm
RUN mkdir -p /var/log/slurm && chown -R slurm:slurm /var/log/slurm
RUN mkdir -p /var/spool/slurmctld && chown -R slurm:slurm /var/spool/slurmctld
RUN mkdir -p /var/spool/slurmd && chown -R slurm:slurm /var/spool/slurmd

FROM base as dbd
# Just the database daemon
RUN apt install -y slurmdbd
COPY entrypoints/dbd-entrypoint.sh /usr/local/bin/dbd-entrypoint.sh
RUN chmod +x /usr/local/bin/dbd-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/dbd-entrypoint.sh"]

FROM base as ctl
# Just the controller
COPY entrypoints/ctl-entrypoint.sh /usr/local/bin/ctl-entrypoint.sh
RUN chmod +x /usr/local/bin/ctl-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/ctl-entrypoint.sh"]

FROM base as worker
# The slurm worker, and base HPC tools like Nextflow
COPY entrypoints/worker-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
ENV SLURM_INCLUDE_DIR=/usr/include
ENV SLURM_LIB_DIR=/usr/lib/x86_64-linux-gnu
RUN DEBIAN_FRONTEND=noninteractive \
    apt -y update && apt install -y default-jre
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

FROM worker as single_node
# DBD + Ctl + Worker in a single container
RUN apt install -y slurmdbd
COPY entrypoints/single-node-entrypoint.sh /usr/local/bin/worker-entrypoint.sh
RUN chmod +x /usr/local/bin/worker-entrypoint.sh
COPY configs/slurm_single_node.conf /etc/slurm/slurm.conf
COPY configs/slurmdbd_single_node.conf /etc/slurm/slurmdbd.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

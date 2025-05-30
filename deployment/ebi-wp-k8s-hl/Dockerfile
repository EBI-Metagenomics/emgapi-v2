FROM python:3.12
LABEL authors="sandyr"

ENV USER=docker
ENV UID=7123
ENV GID=1347
RUN addgroup --gid "$GID" "$USER" \
   && adduser \
   --disabled-password \
   --gecos "" \
   --home "$(pwd)" \
   --ingroup "$USER" \
   --no-create-home \
   --uid "$UID" \
   "$USER"

WORKDIR /app

ADD requirements.txt ./requirements.txt
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

ADD analyses ./analyses
ADD emgapiv2 ./emgapiv2
ADD ena ./ena
ADD static ./static
ADD templates ./templates
ADD --chown=7123:1347 workflows ./workflows
ADD manage.py ./manage.py

ADD .prefectignore .prefectignore

EXPOSE 8000
ENV DJANGO_SETTINGS_MODULE=emgapiv2.settings
ENV PYTHONUNBUFFERED=0

RUN python manage.py collectstatic --noinput
CMD ["gunicorn", "--bind", ":8000", "--workers", "3", "emgapiv2.wsgi"]

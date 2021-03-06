FROM python:3

WORKDIR /usr/src/app

COPY ./src/requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY ./src/. .
# ver como passar a senha de outra forma e mudar o usuário para um específico para o monitor.
ENV CHANGE_STREAM_DB=""
CMD [ "python","-u", "./ChangeMonitor.py" ]


EXPOSE 8011
FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .
# ver como passar a senha de outra forma e mudar o usuário para um específico para o monitor.
ENV CHANGE_STREAM_DB="mongodb://root:mongopass@mongodb/admin?retryWrites=true"
CMD [ "python", "./ChangeMonitor.py" ]


FROM python:3.7
WORKDIR /crypto_ranker
COPY . /crypto_ranker
RUN mkdir -p /root/.cw
COPY src/credentials.yml /root/.cw/credentials.yml
RUN pip3 install -r src/requirements.txt




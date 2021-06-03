FROM python:3.7
WORKDIR /gen_dataset
COPY . /gen_dataset
RUN pip3 install -r src/requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS /gen_dataset/src/key.json

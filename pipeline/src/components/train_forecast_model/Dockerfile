FROM python:3.7
WORKDIR /train_forecast_model
COPY . /train_forecast_model
RUN pip3 install -r src/requirements.txt
ENV GOOGLE_APPLICATION_CREDENTIALS /train_forecast_model/src/key.json
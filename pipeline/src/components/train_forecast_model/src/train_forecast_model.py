import argparse
import logging
from google.cloud import aiplatform



def train_forecast_model(AI_dataset_name):
    logging.basicConfig(level=logging.INFO)
    logging.info('Empieza componente gen dataset ..')
    logging.info('AI dataset name:{}'.format(AI_dataset_name))
    with open(AI_dataset_name) as f:
        dataset_name = f.readline()
    transformations = [
        {"timestamp": {"column_name": "close_ts"}},
        {"numeric": {"column_name": "open_value"}},
        {"numeric": {"column_name": "high_value"}},
        {"numeric": {"column_name": "low_value"}},
        {"numeric": {"column_name": "close_value"}},
        {"numeric": {"column_name": "volume_base"}},
        {"numeric": {"column_name": "volume_quote"}},
    ]
    autoMLForecastJob = aiplatform.AutoMLForecastingTrainingJob(display_name='candles_15min_AutoMLJob',
                                                                column_transformations=transformations,
                                                                optimization_objective="minimize-rmse")
    dataset = aiplatform.TimeSeriesDataset(dataset_name=dataset_name)
    model = autoMLForecastJob.run(dataset=dataset,
                                  target_column="close_value",
                                  time_column="close_ts",
                                  time_series_identifier_column="ticket",
                                  unavailable_at_forecast_columns=["open_value", "high_value", "low_value",
                                                                   "close_value", "volume_base", "volume_quote"],
                                  available_at_forecast_columns=["close_ts"],
                                  forecast_horizon=50,
                                  data_granularity_unit="minute",
                                  data_granularity_count=15,
                                  time_series_attribute_columns=[],
                                  budget_milli_node_hours=1000,
                                  model_display_name="candles_15min_AutoMLModel")


def main(params):
    train_forecast_model(params.AI_dataset_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Entrena un modelo de forecast en Vertex AI')
    parser.add_argument('--AI_dataset_name',  type=str)
    params = parser.parse_args()
    main(params)

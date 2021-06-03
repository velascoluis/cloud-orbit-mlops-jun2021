import argparse
import logging
import cryptowatch as cw
import pandas as pd
import json
import os
from google.cloud import aiplatform
from google.cloud import bigquery
from datetime import datetime, timedelta


def gen_dataset(market_list, project_id, dataset, AI_dataset_name):
    REGION="us-central1"
    logging.basicConfig(level=logging.INFO)
    logging.info('Empieza componente gen dataset ..')
    logging.info('Market list:{}'.format(market_list))
    with open(market_list) as f:
        coin_dict_s = f.readline()
        coin_dict = json.loads(coin_dict_s)

    top_coin = list(coin_dict)[0]
    rows_list = []
    candles = cw.markets.get(top_coin, ohlc=True)
    for x in candles.of_15m:
        close_ts = datetime.utcfromtimestamp(x[0])
        open_value = x[1]
        high_value = x[2]
        low_value = x[3]
        close_value = x[4]
        volume_base = x[5]
        volume_quote = x[6]
        rows_list.append([top_coin, close_ts, open_value, high_value, low_value, close_value, volume_base, volume_quote])
    df = pd.DataFrame(rows_list, columns=["ticket", "close_ts", "open_value", "high_value", "low_value", "close_value",
                                          "volume_base", "volume_quote"])
    client = bigquery.Client()
    TABLE = "temp_data"
    table_id = 'velascoluis-test' + "." + 'crypto_data' + "." + TABLE
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("ticket", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("close_ts", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("open_value", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("high_value", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("low_value", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("close_value", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("volume_base", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("volume_quote", bigquery.enums.SqlTypeNames.FLOAT64)
        ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

    table = client.get_table(table_id)
    print("Operacion OK.  {} filas y {} columnas cargadas en {}".format(table.num_rows, len(table.schema), table_id))
    aiplatform.init(project=project_id, location=REGION)
    dataset_vertex = aiplatform.TimeSeriesDataset.create(display_name=TABLE,bq_source='bq://' + project_id + "." + dataset + "." + TABLE)
    print(dataset_vertex.resource_name)
    if not os.path.exists(os.path.dirname(AI_dataset_name)):
        os.makedirs(os.path.dirname(AI_dataset_name))
    with open(AI_dataset_name, 'w') as f:
        f.write(dataset_vertex.resource_name)


def main(params):
    gen_dataset(params.market_list, params.project_id, params.dataset, params.AI_dataset_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genera el dataset de Vertex AI')
    parser.add_argument('--market_list',  type=str)
    parser.add_argument('--project_id', type=str, default="velascoluis-test")
    parser.add_argument('--dataset', type=str, default="crypto_data")
    parser.add_argument('--AI_dataset_name', type=str, default='None')
    params = parser.parse_args()
    main(params)

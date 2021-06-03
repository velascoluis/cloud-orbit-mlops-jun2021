import kfp
import uuid
import argparse
from google.cloud import aiplatform
from google_cloud_pipeline_components import aiplatform as gcc_aip
from kfp.v2 import compiler
from kfp import components
from kfp.v2.google.client import AIPlatformClient

crypto_ranker_file = 'components/cryto_ranker/component.yaml'
cryptoRankerOp = components.load_component_from_file(crypto_ranker_file)
gen_dataset_file = 'components/gen_dataset/component.yaml'
genDatasetOp = components.load_component_from_file(gen_dataset_file)
train_forecast_model_file = 'components/train_forecast_model/component.yaml'
trainForecastModelOp = components.load_component_from_file(train_forecast_model_file)


def main(params):
    print('Generando y ejecutando el pipeline de KFP ...')
    PROJECT_ID = params.project_id
    REGION = params.region
    PIPELINE_ROOT = params.pipeline_root
    EXCHANGE = params.exchange
    BENEFIT_THRESHOLD = params.benefit_threshold
    DATASET = params.dataset

    @kfp.dsl.pipeline(name="crypto-coins-forecast" + str(uuid.uuid4()))
    def pipeline(project: str = PROJECT_ID):
        crytoRankerTask = cryptoRankerOp(EXCHANGE, BENEFIT_THRESHOLD)
        genDatasetTask = genDatasetOp(crytoRankerTask.outputs['market_list'], PROJECT_ID, DATASET)
        trainForecastModelTask = trainForecastModelOp(genDatasetTask.outputs['AI_dataset_name'])

    print("Compilando ..")
    compiler.Compiler().compile(
        pipeline_func=pipeline, package_path="pipeline_def/crypto_pipeline.json"
    )
    api_client = AIPlatformClient(
        project_id=PROJECT_ID,
        region=REGION,
    )
    print("Ejecutando ..")
    response = api_client.create_run_from_job_spec(
        "pipeline_def/crypto_pipeline.json",
        pipeline_root=PIPELINE_ROOT,
        parameter_values={"project": PROJECT_ID},
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Component-based build-train-deploy pipeline')
    parser.add_argument('--project_id', type=str, default="velascoluis-test")
    parser.add_argument('--pipeline_root', type=str, default="gs://master_bucket_us")
    parser.add_argument('--region', type=str, default="us-central1")
    parser.add_argument('--exchange', type=str, default='coinbase')
    parser.add_argument('--benefit_threshold', type=str, default=5)
    parser.add_argument('--dataset', type=str, default='crypto_data')
    params = parser.parse_args()
    main(params)

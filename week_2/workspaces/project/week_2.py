from datetime import datetime
from typing import List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    String,
    graph,
    op,
)
from workspaces.config import REDIS, S3, S3_FILE
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    description="get stock list from S3 file",
    config_schema={"s3_key": String},
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    stocks = context.resources.s3.get_data(key_name=s3_key)
    return list(map(Stock.from_list, stocks))



@op(description="from a list of stocks as an input return an aggregation with the highest value")
def process_data(context, stocks: List[Stock]) -> Aggregation:
    max_value = max(stocks, key=lambda stock: stock.high)
    ag = Aggregation(date=max_value.date, high=max_value.high)
    return ag


@op(
    description="writes aggregation to redis",
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
)
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(name = str(aggregation.date), value = str(aggregation.high))


@op(
    description="writes aggregation to S3",
    ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(Nothing),
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
)
def put_s3_data(context, aggregation):
    context.resources.s3.put_data(key_name=String(aggregation.date), data=aggregation)


@graph
def machine_learning_graph():
    res = process_data(get_s3_data())
    put_redis_data(res)
    put_s3_data(res)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

docker = {
    "resources": {
        "s3": {"config": S3},
        "redis": {"config": REDIS},
    },
    "ops": {"get_s3_data": {"config": {"s3_key": S3_FILE}}},
}

machine_learning_job_local = machine_learning_graph.to_job(
    name="machine_learning_job_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

machine_learning_job_docker = machine_learning_graph.to_job(
    name="machine_learning_job_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)

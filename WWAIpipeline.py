from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from airflow.utils import timezone

import pandas as pd
from datetime import timedelta
import requests
import json
import sys

#--- Connect to MongoDB ---#

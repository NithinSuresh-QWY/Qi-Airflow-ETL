from airflow.models import Variable
import json

def load_config():
    return json.loads(Variable.get("etl_config_json"))

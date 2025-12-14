import yaml
from pathlib import Path


class Config:
    def __init__(self, data):
        self.api_base_url = data["api_base_url"]
        self.api_timeout = data["api_timeout"]
        self.latitude = data["latitude"]
        self.longitude = data["longitude"]
        self.LSO_URL = data["LSO_URL"]
        self.LSO_SYSTEM_ID = data["LSO_SYSTEM_ID"]


def load_config():
    path = Path("config/settings.yaml")
    with path.open() as f:
        data = yaml.safe_load(f)
    return Config(data)

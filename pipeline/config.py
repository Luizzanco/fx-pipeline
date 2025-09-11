import os
from dotenv import load_dotenv
import yaml
from pathlib import Path

load_dotenv()

def load_config(path: str = "configs/config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    # expand env vars in yaml
    rendered = os.path.expandvars(raw)
    cfg = yaml.safe_load(rendered)
    # ensure folders exist
    for p in (cfg["paths"]["raw"], cfg["paths"]["silver"], cfg["paths"]["gold"]):
        Path(p).mkdir(parents=True, exist_ok=True)
    return cfg

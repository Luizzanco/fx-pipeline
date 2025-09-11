import sys
from pipeline.config import load_config
from pipeline.ingest import ingest_rates
from pipeline.transform import transform_raw_to_silver
from pipeline.load import load_to_gold, load_to_db
from pipeline.utils import setup_logger

logger = setup_logger("run_pipeline")

def run():
    cfg = load_config()
    try:
        raw_file = ingest_rates(cfg)
        silver_file = transform_raw_to_silver(raw_file, cfg)
        gold_file = load_to_gold(silver_file, cfg)
        load_to_db(gold_file, cfg)
    except Exception as e:
        logger.exception("Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    run()

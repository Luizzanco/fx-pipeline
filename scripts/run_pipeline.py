import logging
from pathlib import Path

# Import dos módulos da pipeline
from pipeline.ingest import ingest_rates
from pipeline.transform import transform_raw_to_silver
from pipeline.load import load_to_gold

# Configuração de logging estruturado
logging.basicConfig(
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "name": "%(name)s", "message": "%(message)s"}',
    level=logging.INFO
)
logger = logging.getLogger("pipeline")

# Diretórios padrão
RAW_DIR = Path("data/raw")
SILVER_DIR = Path("data/silver")
GOLD_FILE = Path("data/gold/fx_rates_latest.parquet")

# Cria os diretórios se não existirem
RAW_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
GOLD_FILE.parent.mkdir(parents=True, exist_ok=True)

def run_pipeline(base_currency="USD"):
    """Executa a pipeline FX end-to-end: ingest -> transform -> load"""
    try:
        logger.info("Iniciando pipeline FX")

        # 1. Ingest
        logger.info(f"Iniciando ingest para {base_currency}")
        raw_file = ingest_rates(base_currency=base_currency, output_dir=str(RAW_DIR))
        logger.info(f"Ingest sucesso: {raw_file}")

        # 2. Transform
        silver_file_path = SILVER_DIR / f"fx_rates_{base_currency}.parquet"
        logger.info(f"Iniciando transformação raw -> silver: {silver_file_path}")
        transform_raw_to_silver(str(raw_file), str(silver_file_path))
        logger.info(f"Transform sucesso: {silver_file_path}")

        # 3. Load
        logger.info("Iniciando load para camada gold")
        load_to_gold(silver_file_path, str(GOLD_FILE))
        logger.info(f"Load sucesso: {GOLD_FILE}")

        logger.info("Pipeline concluída com sucesso!")

        return GOLD_FILE

    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")
        raise

if __name__ == "__main__":
    run_pipeline()

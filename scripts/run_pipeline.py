import os
import pandas as pd
from pipeline.config import load_config
from pipeline.ingest import ingest_rates
from pipeline.transform import transform_raw_to_silver
from pipeline.load import load_rates
from pipeline.enrich import generate_summary
from pipeline.logger import log
import openai  # Cliente OpenAI

def run_pipeline():
    try:
        # 1️⃣ Carregar configuração
        cfg = load_config()
        log.info("Iniciando pipeline FX", extra={"event": "pipeline_start"})

        # 2️⃣ Ingestão
        raw_file = ingest_rates(cfg)
        log.info(f"Ingest sucesso: {raw_file}", extra={"event": "ingest_success"})

        # 3️⃣ Transformação
        silver_file = transform_raw_to_silver(raw_file, cfg)
        log.info(f"Transform sucesso: {silver_file}", extra={"event": "transform_success"})

        # Ler parquet para próximos passos
        df_silver = pd.read_parquet(silver_file)

        # 4️⃣ Load
        gold_file = load_rates(df_silver)
        log.info(f"Load sucesso: {gold_file}", extra={"event": "load_success"})

        # 5️⃣ Enriquecimento LLM com tratamento genérico de erros
        summary = "Resumo LLM não gerado."
        if os.path.exists(gold_file):
            df_gold = pd.read_parquet(gold_file)
            try:
                summary = generate_summary(df_gold, base_currency="BRL")
                log.info(f"Resumo LLM gerado:\n{summary}", extra={"event": "llm_success"})
            except Exception as e:
                log.error(f"Erro ao gerar resumo LLM: {e}", extra={"event": "llm_failed"})
                summary = "Resumo LLM não gerado por erro ou limite de cota."
            print("\n📊 Resumo Executivo (LLM):\n")
            print(summary)
        else:
            log.error("Arquivo gold não encontrado.", extra={"event": "llm_failed"})

        log.info("Pipeline finalizada com sucesso", extra={"event": "pipeline_end"})

    except Exception as e:
        log.error(f"Pipeline falhou: {e}", extra={"event": "pipeline_failed"})
        print("❌ Pipeline falhou:", e)


if __name__ == "__main__":
    run_pipeline()

import os
import openai
import pandas as pd
from pipeline.logger import log
from dotenv import load_dotenv

# Carrega variáveis de ambiente do .env
load_dotenv()

# Inicializa cliente OpenAI
openai.api_key = os.getenv("OPENAI_API_KEY")


def generate_summary(df: pd.DataFrame, base_currency="BRL") -> str:
    """
    Gera um resumo executivo em linguagem natural das cotações de moedas usando OpenAI.
    """
    if df.empty:
        log.warning("DataFrame vazio passado para LLM.", extra={"event": "llm_warning"})
        return "Não há dados para gerar resumo."

    # Seleciona as 5 principais moedas por taxa mais alta
    top_currencies = df.sort_values(by="rate", ascending=False).head(5)

    prompt = f"""
    Você é um analista financeiro. Explique de forma simples para um executivo:
    O desempenho das 5 principais moedas em relação ao {base_currency} hoje, 
    com destaque para variações e volatilidade.

    Dados (currency, rate):
    {top_currencies[['currency', 'rate']].to_dict(orient='records')}
    """

    try:
        # Chamada ao OpenAI
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Você é um analista financeiro."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.7
        )

        summary = response.choices[0].message.content.strip()

        # Logging do prompt e da resposta para auditoria
        log.info(f"Prompt enviado ao LLM:\n{prompt}", extra={"event": "llm_prompt"})
        log.info(f"Resposta do LLM:\n{summary}", extra={"event": "llm_response"})

        return summary

    except Exception as e:
        log.error(f"Erro ao gerar resumo LLM: {e}", extra={"event": "llm_failed"})
        return "Resumo LLM não gerado por erro ou limite de cota."

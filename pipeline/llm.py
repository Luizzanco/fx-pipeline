import os
import pandas as pd
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def generate_summary(df: pd.DataFrame, base_currency="USD", local_currency="BRL"):
    """
    Envia dataset para o ChatGPT e gera um resumo executivo em linguagem natural
    """
    # Seleciona apenas algumas moedas para não estourar o prompt
    subset = df[df["currency"].isin(["EUR", "BRL", "GBP", "JPY", "CAD"])]
    text_data = subset.to_dict(orient="records")

    prompt = f"""
    Você é um analista financeiro. Analise o dataset de cotações cambiais 
    baseado na moeda {base_currency} e explique em termos simples para executivos de negócio:

    - Como está a variação das 5 principais moedas frente ao {local_currency} hoje?
    - Destaque se há valorização, desvalorização ou volatilidade acima da média.
    - Seja conciso e objetivo.

    Dataset: {text_data}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=300,
        temperature=0.3
    )

    return response.choices[0].message.content.strip()

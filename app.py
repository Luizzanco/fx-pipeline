import streamlit as st
import pandas as pd
import plotly.express as px
from pipeline.enrich import generate_summary
from pathlib import Path

st.set_page_config(page_title="FX Pipeline Dashboard", layout="wide")

st.title("💱 FX Pipeline - Dashboard")

# Caminhos
GOLD_FILE = "data/gold/fx_rates_latest.parquet"
SILVER_DIR = Path("data/silver")

# Botão para carregar dados
if st.button("Carregar cotações"):
    try:
        # Lê gold
        df = pd.read_parquet(GOLD_FILE)
        st.subheader("📊 Taxas de câmbio atuais")
        st.dataframe(df)

        # Resumo LLM
        st.subheader("📝 Resumo Executivo (LLM)")
        summary = generate_summary(df, base_currency="BRL")
        st.text(summary)

        # Gráfico de evolução (comparando último silver)
        silver_files = sorted(SILVER_DIR.glob("*.parquet"))
        if silver_files:
            df_prev = pd.read_parquet(silver_files[-1])
            merged = pd.merge(df_prev, df, on="currency", suffixes=("_prev", "_current"))
            merged["diff"] = merged["rate_current"] - merged["rate_prev"]
            merged["color"] = merged["diff"].apply(lambda x: "green" if x > 0 else "red")

            st.subheader("📈 Evolução das moedas (último período)")
            fig = px.bar(
                merged,
                x="currency",
                y="diff",
                color="color",
                color_discrete_map={"green":"green", "red":"red"},
                title="Variação das moedas vs período anterior"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nenhum arquivo silver anterior encontrado para comparação.")

    except FileNotFoundError:
        st.error("Arquivo gold não encontrado. Rode o pipeline primeiro.")
    except Exception as e:
        st.error(f"Ocorreu um erro: {e}")

from pipeline.enrich import generate_summary

def test_generate_summary_returns_string():
    # Cria DataFrame fake
    import pandas as pd
    df = pd.DataFrame({"currency": ["EUR", "BRL"], "rate": [0.9, 5.0]})

    # Gera resumo
    summary = generate_summary(df)

    # Verifica se é uma string não vazia
    assert isinstance(summary, str)
    assert len(summary) > 0

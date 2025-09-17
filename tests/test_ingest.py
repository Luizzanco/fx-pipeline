import os
from pipeline.ingest import ingest_rates

def test_ingest_rates_creates_file(tmp_path):
    # Simula diretório de saída
    output_dir = tmp_path / "raw"
    output_dir.mkdir()

    # Executa ingest
    file_path = ingest_rates("USD", str(output_dir))

    # Valida se o arquivo foi criado
    assert os.path.exists(file_path)
    assert file_path.endswith(".json")

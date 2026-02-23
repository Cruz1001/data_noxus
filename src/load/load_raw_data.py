import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import pandas as pd
import traceback

load_dotenv()

FILE_ID = "1hnpbrUpBMS1TZI7IovfpKeZfWJH1Aptm"

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:" 
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:" 
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

def load_raw(df: pd.DataFrame):
    engine = get_engine()
    
    # Truncar a tabela
    print("Truncando staging.raw_matches...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.raw_matches;"))
    
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64', 'int32', 'float32']:
                df[col] = df[col].fillna(0)
            else:
                df[col] = df[col].fillna("")
        
    # 2️⃣ Converter objetos complexos para string
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str)
    
    column_map = {
    "firstPick": "firstpick",
    "team kpm": "team_kpm",
    "dragons (type unknown)": "dragons_type_unknown",
    "earned gpm": "earned_gpm",
    "total cs": "total_cs"}
    column_map = {col: column_map.get(col, col) for col in df.columns}
    
    # Aplicando o map
    df = df.rename(columns=column_map)
    # Converte colunas booleanas
    bool_columns = ["playoffs", "firstpick"]  # adicione mais se houver
    for col in bool_columns:
        if col in df.columns:
            df[col] = df[col].map({0: False, 1: True, "0": False, "1": True, "False": False, "True": True})
    # Inserir dados com tratamento de erro
    print("Inserindo dados...")
    try:
        df.to_sql(
            "raw_matches",
            engine,
            schema="staging",
            if_exists="append",
            index=False,
            chunksize=100,
            method="multi"
        )
        print("Carga finalizada 🚀")
    except Exception as e:
        print("❌ Erro ao inserir dados!")
        # Mostrar apenas o tipo do erro e a primeira linha problemática
        print(f"Tipo do erro: {type(e).__name__}")
        print(f"Mensagem: {str(e)[:200]}...")  # corta a mensagem gigante
        # Tentar identificar a primeira linha que deu problema
        for i, row in df.iterrows():
            try:
                row.to_frame().T.to_sql(
                    "raw_matches",
                    engine,
                    schema="staging",
                    if_exists="append",
                    index=False,
                    method="multi"
                )
            except Exception:
                print(f"Erro na linha {i}: {row.to_dict()}")
                break
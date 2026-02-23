from extract.sheets_extractor import extract_sheets
from load.load_raw_data import load_raw

def main():

    print("Iniciando pipeline ELT...")

    print("Extraindo dados do Drive...")
    df = extract_sheets()

    print(f"{len(df)} linhas extraídas.")

    print("Carregando no staging.raw_matches...")
    load_raw(df)

    print("Pipeline finalizada com sucesso 🚀")


if __name__ == "__main__":
    main()
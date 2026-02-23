import os 
import pandas as pd

def extract_sheets() -> pd.DataFrame:
    """
    Extracts the sheets data from the Excel file and returns it as a DataFrame.
    """
    file_url = "https://drive.google.com/uc?export=download&id=1hnpbrUpBMS1TZI7IovfpKeZfWJH1Aptm"
    
    # Read the Excel file into a DataFrame
    sheets_df = pd.read_csv(file_url)
    
    return sheets_df

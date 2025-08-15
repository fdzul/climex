import pandas as pd

def load_centroid_data(path: str, state: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={'CVEGEO': str})
    df['edo'] = df['CVEGEO'].str[-3:]
    return df[df['edo'] == state].drop(columns=['edo'])
from typing import List
import pandas as pd
from io import BytesIO, StringIO

def load_file_to_dataframe(filename: str, content: bytes, headers: List [str]) -> pd.DataFrame:
    filename = filename.lower()

    if filename.endswith(".csv"):
        return pd.read_csv(StringIO(content.decode("utf-8")), names=headers)
    if filename.endswith(".xlsx") or filename.endswith(".xls"):
        return pd.read_excel(BytesIO(content), names=headers)
    if filename.endswith(".txt"):
        return pd.read_csv(StringIO(content.decode("utf-8")), sep="\t", names=headers)

    raise ValueError("Nieobsługiwany format plików")
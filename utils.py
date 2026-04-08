import pandas as pd

def read_file(uploaded_file):
    file_type = uploaded_file.name.split(".")[-1]

    if file_type == "csv":
        return pd.read_csv(uploaded_file)

    elif file_type == "json":
        return pd.read_json(uploaded_file)

    elif file_type == "xlsx":
        return pd.read_excel(uploaded_file)

    else:
        raise ValueError("Unsupported file format")
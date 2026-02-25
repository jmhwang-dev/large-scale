import pandas as pd
import glob
import os

"""
This script just create 
"""

SRC_PATH = "./artifact/medallion/bronze/olist/*"
DST_PATH = "./eda/erd_tmp.dbml"

def dataframe_to_dbml(df:pd.DataFrame, table_name:str) -> str:
    dbml = f"Table {table_name} {{\n"
    for column in df.columns:
        dtype = str(df[column].dtype)
        if "int" in dtype:
            dbml_type = "int"
        elif "float" in dtype:
            dbml_type = "decimal"
        elif "object" in dtype:
            dbml_type = "varchar"
        else:
            dbml_type = "varchar"  # 기본값
        dbml += f"\t{column} {dbml_type}\n"
    dbml += "}"
    return dbml

if __name__=="__main__":
    csv_paths = glob.glob(SRC_PATH)
    csv_paths.sort()
    csv_paths

    with open(DST_PATH, 'w') as f:
        f.write('')

    for csv_path in csv_paths:
        df = pd.read_csv(csv_path)
    
        file_name = os.path.split(csv_path)[1]
        table_name = os.path.splitext(file_name)[0]

        dbml_schema = dataframe_to_dbml(df, table_name)
        dbml_schema += '\n'

        with open(DST_PATH, 'a') as f:
            f.write(dbml_schema)

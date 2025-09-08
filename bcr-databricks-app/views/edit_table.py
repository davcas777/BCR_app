import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

cfg = Config()

@st.cache_resource
def get_connection(http_path):
    return sql.connect(server_hostname=cfg.host, http_path=http_path, credentials_provider=lambda: cfg.authenticate)

def read_table(table_name, conn):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        return cursor.fetchall_arrow().to_pandas()

def insert_overwrite_table(table_name, df, conn):
    progress = st.empty()
    with conn.cursor() as cursor:
        rows = list(df.itertuples(index=False))
        values = ",".join([f"({','.join(map(repr, row))})" for row in rows])
        with progress:
            st.info("Calling Databricks SQL...")
        cursor.execute(f"INSERT OVERWRITE {table_name} VALUES {values}")
    progress.empty()
    st.success("Changes saved")

def run():
    st.image("assets/logo.svg", width=100)
    st.header("Edit Delta Tables")
    http_path_input = st.text_input("Databricks HTTP Path:")
    table_name = st.text_input("Delta Table Name:")
    if http_path_input and table_name:
        conn = get_connection(http_path_input)
        df = read_table(table_name, conn)
        edited_df = st.data_editor(df, num_rows="dynamic", hide_index=True)
        if st.button("Save changes") and not df.equals(edited_df):
            insert_overwrite_table(table_name, edited_df, conn)
            st.success("Table updated!")
    else:
        st.warning("Provide both HTTP path and table name.")

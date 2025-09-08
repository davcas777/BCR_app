import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
import pandas as pd

cfg = Config()

@st.cache_resource
def get_connection(http_path):
    return sql.connect(server_hostname=cfg.host, http_path=http_path, credentials_provider=lambda: cfg.authenticate)

def export_table(table_name, conn, fmt):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name}")
        df = cursor.fetchall_arrow().to_pandas()
    if fmt == "CSV":
        return df.to_csv(index=False).encode("utf-8"), "export.csv"
    elif fmt == "XML":
        xml_content = "<rows>\n" + "\n".join(
            ["  <row>" + "".join([f"<{col}>{row[col]}</{col}>" for col in df.columns]) + "</row>" for _, row in df.iterrows()]
        ) + "\n</rows>"
        return xml_content.encode("utf-8"), "export.xml"
    else:
        st.error("Export format not supported.")
        return None, None

def run():
    st.image("assets/logo.svg", width=100)
    st.header("Export Gold Delta Table")
    http_path_input = st.text_input("HTTP Path to Databricks SQL Warehouse:")
    table_name = st.text_input("Gold Table Name:")
    export_format = st.selectbox("Export Format:", ["CSV", "XML"])
    if http_path_input and table_name and st.button("Download"):
        conn = get_connection(http_path_input)
        file, name = export_table(table_name, conn, export_format)
        if file:
            st.download_button("Download file", file, file_name=name)

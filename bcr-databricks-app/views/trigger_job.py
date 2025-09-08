import streamlit as st
from databricks.sdk import WorkspaceClient

def run():
    st.image("assets/logo.svg", width=100)
    st.header("Trigger Databricks Quality Jobs")
    w = WorkspaceClient()
    job_id = st.text_input("Job ID:", help="Find job ID in Databricks UI.")
    params_str = st.text_area("Job Parameters (JSON):", value="{}")
    try:
        parameters = eval(params_str.strip())
    except Exception:
        parameters = {}
    if st.button("Trigger job"):
        try:
            run_obj = w.jobs.run_now(job_id=job_id, job_parameters=parameters)
            st.success(f"Started run with ID {run_obj.run_id}")
        except Exception as e:
            st.warning(str(e))

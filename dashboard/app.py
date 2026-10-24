"""dashboard/app.py — Streamlit entrypoint. Navigation only.

Run from the project root:
    PYTHONPATH=/Users/rhlbh/projects/claudeProjects/quant \
      /Users/rhlbh/projects/claudeProjects/bin/python -m streamlit run dashboard/app.py
"""

import streamlit as st

st.set_page_config(page_title="Quant Research", layout="wide")

st.navigation({
    "Research": [
        st.Page("research/experiments.py", title="Experiments", default=True),
        st.Page("research/experiment.py", title="Experiment", url_path="experiment", visibility="hidden"),
    ],
    "Macro": [
        st.Page("macro/state.py", title="Macro state", url_path="macro-state"),
        st.Page("macro/explorer.py", title="Macro explorer", url_path="macro"),
    ],
}).run()

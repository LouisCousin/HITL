import asyncio
import json
from pathlib import Path

import streamlit as st

from openai_scheduler import SchedulerConfig, OpenAIScheduler

st.set_page_config(page_title="OpenAI Scheduler", page_icon="🤖")
st.title("OpenAI Scheduler")

source = st.radio("Source des chunks", ["Fichier", "Texte"], index=0)
chunks = []
if source == "Fichier":
    uploaded = st.file_uploader("Fichier texte", type=["txt"])
    if uploaded is not None:
        content = uploaded.read().decode("utf-8")
        chunks = [line.strip() for line in content.splitlines() if line.strip()]
else:
    text = st.text_area("Entrez un chunk par ligne")
    if text:
        chunks = [line.strip() for line in text.splitlines() if line.strip()]

api_key = st.text_input("Clé API OpenAI", type="password")

st.sidebar.header("Configuration")
max_concurrent = st.sidebar.number_input("Concurrence max", value=15, min_value=1)
interval = st.sidebar.number_input("Intervalle entre envois (s)", value=4.0, min_value=0.1, step=0.1)
backoff = st.sidebar.number_input("Backoff initial (s)", value=1.0, min_value=0.1, step=0.1)
retries = st.sidebar.number_input("Nombre de retries", value=3, min_value=0)
progress_file = st.sidebar.text_input("Fichier de progrès", value="progress.txt")

st.sidebar.subheader("Paramètres OpenAI")
model = st.sidebar.text_input("Modèle", value="gpt-3.5-turbo")
temperature = st.sidebar.slider("Température", 0.0, 2.0, 1.0, 0.1)
top_p = st.sidebar.slider("Top P", 0.0, 1.0, 1.0, 0.05)
presence_penalty = st.sidebar.slider("Presence penalty", -2.0, 2.0, 0.0, 0.1)
frequency_penalty = st.sidebar.slider("Frequency penalty", -2.0, 2.0, 0.0, 0.1)
max_tokens = st.sidebar.number_input("Max tokens", value=0, min_value=0)

run = st.button("Lancer")

if run:
    if not chunks:
        st.error("Aucun chunk fourni")
    elif not api_key:
        st.error("Clé API requise")
    else:
        results = []
        progress_bar = st.progress(0)
        status = st.empty()

        def handler(idx, response):
            if hasattr(response, "choices"):
                content = response.choices[0].message.content
            else:
                content = str(response)
            results.append({"index": idx, "content": content})
            progress_bar.progress((idx + 1) / len(chunks))
            status.text(f"Chunk {idx + 1}/{len(chunks)} terminé")

        config = SchedulerConfig(
            max_concurrent=max_concurrent,
            send_interval=interval,
            backoff_start=backoff,
            max_retries=retries,
            api_key=api_key,
            progress_file=Path(progress_file),
            model=model,
            temperature=temperature,
            top_p=top_p,
            presence_penalty=presence_penalty,
            frequency_penalty=frequency_penalty,
            max_tokens=max_tokens if max_tokens > 0 else None,
        )

        scheduler = OpenAIScheduler(config, handler)
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(scheduler.run(chunks))
        finally:
            loop.close()
        progress_bar.progress(1.0)
        st.success("Traitement terminé")
        st.json(results)
        st.download_button("Télécharger les résultats", json.dumps(results, indent=2), "results.json")

import streamlit as st
import time
import random

st.title("Streaming Bot")


def _streaming_response_generator():
    choices = [
        "Hello there! How can I help you?",
        "Good morning! Is there anything I can assist you?",
        "How are you? Thanks for reaching out!"
    ]
    choice = random.choice(choices)

    for word in choice.split():
        yield word + " "
        time.sleep(0.05)


if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

prompt = st.chat_input("Please type something here...")

if prompt:
    with st.chat_message(name="user"):
        st.markdown(prompt)

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message(name="assistant"):
        response = st.write_stream(_streaming_response_generator())

    st.session_state.messages.append({"role": "assistant", "content": response})

import streamlit as st
import os
import time
from openai import OpenAI

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)


def _streaming_response_generator(stream, delay=0.05):
    for word in stream.split():
        yield word + " "
        time.sleep(delay)

st.title("ChatGPT Bot")

if "openai_model" not in st.session_state:
    st.session_state["openai_model"] = "gpt-3.5-turbo"

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(name=message["role"]):
        st.markdown(message["content"])

prompt = st.chat_input("Please type something here...")

if prompt:
    with st.chat_message(name="user"):
        st.markdown(prompt)

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message(name="assistant"):
        messages = [{"role": message["role"], "content": message["content"]} for message in st.session_state.messages]
        # print(messages)
        completions = client.chat.completions.create(
            model=st.session_state["openai_model"],
            temperature=0.5,
            max_tokens=100,
            messages=messages,
            # stream=True,
        )

        response = st.write_stream(_streaming_response_generator(completions.choices[0].message.content))

        # completions = client.chat.completions.create(
        #     model=st.session_state["openai_model"],
        #     temperature=0.5,
        #     max_tokens=100,
        #     messages=messages,
        #     stream=True,
        # )
        # response = st.write_stream(stream=completions)

    st.session_state.messages.append({"role": "assistant", "content": response})

import streamlit as st

st.title("Echo Bot")

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

    response = f"Echo {prompt}"
    with st.chat_message(name="assistant"):
        st.markdown(response)

    st.session_state.messages.append({"role": "assistant", "content": response})

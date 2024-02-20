import streamlit as st
import pandas as pd
from faker import Faker

fake = Faker()

@st.cache_data(ttl=600)
def load_data(rows=50):
    # Generate fake data
    data = {
        'Name': [fake.name() for _ in range(rows)],
        'Email': [fake.email() for _ in range(rows)],
        'Address': [fake.address() for _ in range(rows)],
    }

    return pd.DataFrame(data)


df = load_data(10)
st.title("Show dataframe")
st.dataframe(df)

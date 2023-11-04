import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Create a Pandas DataFrame with random data
data = {
    'X': np.random.rand(50),
    'Y': np.random.rand(50)
}
df = pd.DataFrame(data)

# Streamlit app code
st.title('Pandas DataFrame and Matplotlib Plot Example')

# Display the DataFrame
st.write("Sample DataFrame:")
st.write(df)

# Create a Matplotlib plot and display it
st.write("Matplotlib Plot:")
fig, ax = plt.subplots()
ax.scatter(df['X'], df['Y'])
ax.set_xlabel('X-axis')
ax.set_ylabel('Y-axis')
st.pyplot(fig)

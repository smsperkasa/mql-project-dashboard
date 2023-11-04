# Use an official Python runtime as a parent image
FROM python:3.9.10

# Set the working directory in the container
WORKDIR /project_dashboard

# Copy the requirements file into the container at /project_dashboard
COPY requirements.txt /project_dashboard/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . /project_dashboard/

# Run the Streamlit app when the container launches
CMD ["streamlit", "run", "project_dashboard.py"]

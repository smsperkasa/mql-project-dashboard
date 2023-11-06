# Use an official Python runtime as a parent image
FROM python:3.9.10

RUN apt-get update && apt-get -y install cron

# Set the working directory in the container
WORKDIR /project_dashboard

# Copy the requirements file into the container at /project_dashboard
COPY requirements.txt /project_dashboard/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . /project_dashboard/

# Copy the cron file to the cron.d directory
COPY cronjob /etc/cron.d/cronjob

# Give execution rights to the cron job
RUN chmod 0644 /etc/cron.d/cronjob

# Create a log file for cron
RUN touch /var/log/cron.log

# Run the command on container startup
CMD ["sh", "-c", "cron && streamlit run project_dashboard.py"]

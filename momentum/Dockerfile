# Start from python slim docker image
FROM python:3.11.8-slim
# Update base packages
RUN apt-get update -y && apt-get upgrade -y
# Change TimeZone to IST (candles / month boundaries are in exchange time)
ENV TZ=Asia/Calcutta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Non-secret defaults (secrets - CONNECTION_STRING, INTEGRATE_*, TOTP, slack_token -
# are injected at runtime by the Container App job, never baked into the image).
# LIVE stays 0 here; set LIVE=1 in the job env only after go-live sign-off.
ENV MONGO_DB="Bots"
ENV user_name="sugam"
ENV LIVE="0"

# Copy the job into the image
COPY . /app/src/
WORKDIR /app
# Install python packages using requirements.txt (all deps declared here)
RUN pip install -r src/requirements.txt

# MOMENTUM runs once, at/just after the open, and is the ONLY job that trades
CMD python src/momentum.py

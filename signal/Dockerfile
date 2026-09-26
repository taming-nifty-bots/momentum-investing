# Start from python slim docker image
FROM python:3.11.8-slim
# Update base packages
RUN apt-get update -y && apt-get upgrade -y
# Change TimeZone to IST (candles / month boundaries are in exchange time)
ENV TZ=Asia/Calcutta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Non-secret defaults (secrets - CONNECTION_STRING, DHAN_CLIENT_ID, DHAN_PIN,
# DHAN_TOTP, slack_token - are injected at runtime by the Container App job,
# never baked into the image).
ENV MONGO_DB="Bots"
ENV user_name="sugam"

# Copy the job into the image
COPY . /app/src/
WORKDIR /app
# Install python packages using requirements.txt (all deps declared here)
RUN pip install -r src/requirements.txt

# SIGNAL places NO orders (analysis only). It runs the MORNING AFTER the session
# it acts on, because Dhan publishes the daily candle too late for an evening run.
CMD python src/momentum_signal.py

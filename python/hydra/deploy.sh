#!/bin/bash

# Check if a command exists
command_exists() {
    type "$1" &> /dev/null
}

# Check for jq and install if not found
if ! command_exists jq; then
    echo "jq not found, attempting to install..."
    sudo apt-get update && sudo apt-get install -y jq
fi

# Check for gcloud and install if not found
if ! command_exists gcloud; then
    echo "Google Cloud SDK not found, attempting to install..."
    # Install Google Cloud SDK (adjust this command if not using a Debian-based system)
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
    sudo apt-get update && sudo apt-get install -y google-cloud-sdk
fi

# Function to retrieve and parse the secret containing JSON configuration
get_config() {
    gcloud secrets versions access latest --secret="$1" | jq .
}

# Retrieve the configuration as a JSON object
CONFIG=$(get_config "hydra_config")

# Use `jq` to extract individual values from the JSON object
SERVICE_NAME=$(echo "$CONFIG" | jq -r '.SERVICE_NAME')
PROJECT_ID=$(echo "$CONFIG" | jq -r '.PROJECT_ID')
DISPATCHER_TOPIC=$(echo "$CONFIG" | jq -r '.DISPATCHER_TOPIC')
WORKER_TOPIC=$(echo "$CONFIG" | jq -r '.WORKER_TOPIC')
SUCCESS_TOPIC=$(echo "$CONFIG" | jq -r '.SUCCESS_TOPIC')
FAILED_TOPIC=$(echo "$CONFIG" | jq -r '.FAILED_TOPIC')
IMAGE_NAME=$(echo "$CONFIG" | jq -r '.IMAGE_NAME')


# Create Pub/Sub topics
echo "Creating Pub/Sub topics..."
gcloud pubsub topics create $DISPATCHER_TOPIC
gcloud pubsub topics create $WORKER_TOPIC
gcloud pubsub topics create $SUCCESS_TOPIC
gcloud pubsub topics create $FAILED_TOPIC
echo "Pub/Sub topics created successfully."

# Create Pub/Sub subscriptions
echo "Creating Pub/Sub subscriptions..."
gcloud pubsub subscriptions create dispatcher-subscription --topic=$DISPATCHER_TOPIC
gcloud pubsub subscriptions create worker-subscription --topic=$WORKER_TOPIC
gcloud pubsub subscriptions create success-subscription --topic=$SUCCESS_TOPIC
gcloud pubsub subscriptions create failure-subscription --topic=$FAILURE_TOPIC
echo "Pub/Sub subscriptions created successfully."

# Deploy Flask application to Google Cloud Run
echo "Deploying Flask application to Google Cloud Run..."
cd controller
docker build -t $IMAGE_NAME .
gcloud run deploy $SERVICE_NAME --image $IMAGE_NAME --platform managed
echo "Flask application deployed successfully."

# Deploy Cloud Functions
echo "Deploying Cloud Functions..."
# Dispatcher Function
gcloud functions deploy dispatcher \
    --runtime python39 \
    --trigger-topic $DISPATCHER_TOPIC \
    --memory 128MB \
    --timeout 60s \
    --entry-point dispatcher_function \
    --source /dispatch \
    --quiet
echo "Dispatcher function deployed successfully."

# Receiver Function
gcloud functions deploy receiver \
    --runtime python39 \
    --trigger-topic $WORKER_TOPIC \
    --memory 128MB \
    --timeout 60s \
    --entry-point receiver_function \
    --source /receiver \
    --quiet
echo "Receiver function deployed successfully."

# Worker Function
gcloud functions deploy worker \
    --runtime python39 \
    --trigger-topic $SUCCESS_TOPIC \
    --memory 256MB \
    --timeout 540s \
    --entry-point worker_function \
    --source /worker \
    --quiet
echo "Worker function deployed successfully."

echo "All components and Pub/Sub resources deployed. Application is live."

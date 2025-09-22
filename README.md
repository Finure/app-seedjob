# Finure App Seedjob

## 1. What it is
This app is part of Finure app project. It is designed to bootstrap data into a Kafka topic by downloading a public dataset from Kaggle (credit approval data) and streaming it to Kafka in batches. The application is intended to run as a Kubernetes Job within a k8s cluster, leveraging Helm charts for deployment and configuration.

## 2. Features
- **Automated Data Bootstrapping:** Downloads public datasets (e.g., Kaggle credit approval data) and streams them to Kafka
- **Kafka Integration:** Securely connects to Kafka using SSL and streams data in batches for efficient ingestion
- **Kubernetes Native:** Includes Helm charts and manifests for easy deployment as a Job in a k8s cluster
- **Environment Configuration:** Supports environment-specific values for flexible deployments
- **Istio Integration:** Script to gracefully exit Istio sidecar once the job is complete

## 3. Prerequisites
- Bootstrap the Kubernetes cluster using the Terraform code located [here](https://github.com/finure/terraform)
- Allow Flux to setup the required infrastructure, including Kafka, code is located [here](https://github.com/finure/kubernetes)

If running locally for development/testing:
- Docker
- Python 3.12+ 
- Kaggle API credentials (optional)

## 4. File Structure
```
app-seedjob/
├── app/
│   ├── kafka-check.py         # Readiness & Liveness probe script for Kubernetes
│   ├── produce_data.py        # Downloads Kaggle dataset and streams to Kafka
│   └── requirements.txt       # Python dependencies
├── k8s/
│   ├── environments/
│   │   └── production/
│   │       └── values.yaml    # Helm production environment values
│   ├── helm-charts/
│   │   └── app-seedjob/
│   │       ├── Chart.yaml     # Helm chart metadata
│   │       ├── values.yaml    # Default Helm values
│   │       └── templates/
│   │           ├── _helpers.tpl # Helm template helpers
│   │           └── job.yaml   # Kubernetes Job manifest
│   └── scripts/
│       └── istio.sh           # Istio graceful exit script
├── Dockerfile                 # Container build file
├── .dockerignore              # Docker ignore rules
├── .gitignore                 # Git ignore rules
└── README.md                  # Project documentation
```

## 5. How to Run Manually

> **Note:** Manual execution is for development/testing only. Production use is via Kubernetes Job.

1. Install Python dependencies:
	```bash
	cd app-seedjob/app
	pip install -r requirements.txt
	```
2. Set required environment variables (see code for details):
	- `KAFKA_BOOTSTRAP_SERVERS`
	- `KAFKA_TOPIC`
	- Ensure Kafka SSL secrets are available at `/etc/kafka/secrets` and `/etc/kafka/ca` (or update paths in code)
3. Run the data producer:
	```bash
	python produce_data.py
	```
	This will download the Kaggle dataset and stream records to the configured Kafka topic.

## 6. k8s Folder Significance

The `k8s` folder contains all Kubernetes-related resources:
- **Helm Charts:** Used to deploy the seed job as a Kubernetes Job in the cluster. Not intended for standalone or local use.
- **Environment Values:** Customize deployments for different environments (e.g., production)
- **Scripts:** Utility scripts for cluster setup (e.g., Istio service mesh)

> **Important:** The resources in `k8s` are designed to be consumed by the Kubernetes cluster during automated deployments. They are not meant for manual execution outside the cluster context.

## Additional Information

This repo is primarly designed to be used in the Finure project. The chart can be used independently by providing the necessary configurations and dependencies however, it is recommended to be used with the Finure application only.
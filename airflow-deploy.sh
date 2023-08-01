curl -sSL install.astronomer.io | sudo bash -s
curl -sSL install.astronomer.io | sudo bash -s -- v1.17.1
astro dev init
astro dev start
gcloud config set project playground-s-11-92fa6a6e
gcloud services enable serviceusage.googleapis.com
gcloud services enable iam.serviceAccounts.create
gcloud services enable iam.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud config set compute/zone us-central1 
gcloud services enable eventarc.googleapis.com
gcloud services enable run.googleapis.com

gcloud functions deploy ingestion \
--runtime=python39 \
--region=us-central1 \
--source=. \
--entry-point=gcs_trigger_xlsx_loader \
--allow-unauthenticated \
--memory=8192MB \
--max-instances=3 \
--trigger-bucket=playground-s-11-92fa6a6e-data

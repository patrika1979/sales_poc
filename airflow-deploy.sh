#change project-id below

#mudar projeto abaixo
gcloud config set project playground-s-11-b1c8573c

gcloud services enable serviceusage.googleapis.com
gcloud services enable iam.serviceAccounts.create
gcloud services enable iam.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud config set compute/zone us-central1-a 
gcloud services enable eventarc.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable storage.objects.create
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable cloudbuild.googleapis.com

cd case_botiario
curl -sSL install.astronomer.io | sudo bash -s
astro dev init
astro dev start

delete dags/examples

cd infra
#change project_id inside terraform.tfvars
#delete terraform state files
terraform plan 
terraform apply

#create bucket
gcloud storage buckets create gs://$DEVSHELL_PROJECT_ID-data --location US-CENTRAL1

#chanege in folder functions
#change project_name_param to project_id
#change trigger-bucket param below

touch init.empty

gsutil cp -r ../init.empty gs://$DEVSHELL_PROJECT_ID-data/Output_path/

cd functions
#change project_name in the code 
#change bucket in the parameter below

#deploy function
gcloud functions deploy ingestion_vendas \
--runtime=python39 \
--region=us-central1 \
--source=. \
--entry-point=gcs_trigger_xlsx_loader \
--allow-unauthenticated \
--memory=8192MB \
--max-instances=3 \
--trigger-bucket=playground-s-11-b1c8573c-data

#open Airflow
#create two airflow connections
bigquery_default
google_cloud_default

#go to IAm and then left menu - service account and copy the key to be paste in the keyjson in the connection

#load data into bucket to trigger function
gsutil cp -r data/* gs://$DEVSHELL_PROJECT_ID-data/Source_path/

#got to dag and change project-id and bucket name


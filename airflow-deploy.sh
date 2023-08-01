curl -sSL install.astronomer.io | sudo bash -s
curl -sSL install.astronomer.io | sudo bash -s -- v1.17.1
astro dev init
astro dev start
gcloud config set project playground-s-11-4c932a0d
#ativar service usage api
gcloud services enable serviceusage.googleapis.com
gcloud services enable iam.serviceAccounts.create
gcloud services enable iam.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud config set compute/zone us-central1 
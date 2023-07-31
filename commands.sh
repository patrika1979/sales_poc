gcloud storage buckets create gs://$DEVSHELL_PROJECT_ID-data --location US-CENTRAL1
gsutil cp -r data/* gs://$DEVSHELL_PROJECT_ID-data/vendas/

airflow connections add 'bigquery' \
    --conn-json '{
        "conn_type": "Google Cloud Platform",
        "Keyfile JSON": 
}
}
   

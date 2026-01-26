python3 workcount.py \
  --runner DataflowRunner \
  --project ordinal-rig-485316-r2 \
  --region us-central1 \
  --staging_location gs://gcs-bucket-curso-05-prueba/staging \
  --temp_location gs://gcs-bucket-curso-05-prueba/temp \
  --template_location gs://gcs-bucket-curso-05-prueba/templates/wordcount_template


pip install apache-beam==2.47.0
pip install protobuf==3.20.1
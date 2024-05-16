import pytz
import datetime
import pandas as pd
from google.cloud import bigquery 
from google.oauth2 import service_account


# -------------------------------  BigQuery  -------------------------------
key_path = 'json_file_path'  # json file from BQ with admin privilege (should contain job creation, data write to BQ)
project_id = ''
dataset_id = ''
table = ''
table_id = "{}.{}.{}".format(project_id, dataset_id, table)
df = ''  # <------your python dataframe 



# editing DataFrame
filtered_df = df[['Date', "Business Line", "Partner", "VIEWS"]]
filtered_df.rename(columns={"Business Line": "Business_Line"}, inplace = True)

# BigQuery Connection
credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(credentials=credentials, project=project_id)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

# Writing data to BQ table
job = client.load_table_from_dataframe(filtered_df, table_id, job_config=job_config)
job.result()

data = client.get_table(table_id)
print(data)




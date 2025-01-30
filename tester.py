from rucio.client import Client, uploadclient
from constants import *
import os
# Initialize the Rucio client
client = Client()
uploadclient = uploadclient.UploadClient()

# Define RSE (storage element), dataset, and file details
rse =  RSE # Replace with your RSE
dataset = "CMIP6.ScenarioMIP.CMCC.CMCC-ESM2.ssp585.r1i1p1f1.day.hur.gn.v20210126"  # Existing dataset or new one
scope = SCOPE  # Rucio scope
file_path = "/pv/jhub-claim-data-pvc-3f444961-9c92-4c9b-93d1-3d3005804576/CMIP6/ScenarioMIP/CMCC/CMCC-ESM2/ssp585/r1i1p1f1/day/hur/gn/v20210126"
file_name = "hur_day_CMCC-ESM2_ssp585_r1i1p1f1_gn_20550101-20561231.nc"  # Name of the file
full_path = os.path.join(file_path, file_name)
did = f"{scope}:{dataset}"

# Create a dataset (if it doesn't exist)
try:
    client.add_dataset(scope, dataset)
    print(f"Dataset {did} created.")
except Exception as e:
    print(f"Dataset may already exist: {e}")

# Upload file to Rucio
files = [
    {
        "did_scope": scope,
        "did_name": file_name,
        "path": full_path,
        "rse": rse
    }
]

try:
    uploadclient.upload(files)
    print(f"File {file_name} uploaded successfully to {rse}.")
except Exception as e:
    print(f"Upload failed: {e}")

# Attach file to dataset
try:
    # client.add_files_to_dataset(scope, dataset, files)
    client.add_files_to_dataset(scope, dataset, [{"scope": scope, "name": file_name}])
    print(f"File {file_name} added to dataset {did}.")
except Exception as e:
    print(f"Failed to attach file to dataset: {e}")
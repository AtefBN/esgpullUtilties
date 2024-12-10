import rucio.client
from rucio.client import Client, uploadclient
from rucio.client.uploadclient import UploadClient
from rucio.common.exception import DataIdentifierAlreadyExists
import json, os


import constants
from utils import parse_file_json, get_files_from_esgpull_query
from constants import RSE, SCOPE


def get_dataset_list(file_path):
    """
    Reads JSON file, parses dictionary and lists dataset identifiers with the attached files.
    Returns: Dictionary with key as dataset_id and value as list of files.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)

    dataset_dict = {}
    data = data['49d8b79df01e29fa065ce9d65211d03e98b19750']['files']
    for file in data:
        dataset_id = file['dataset_id']
        # file_id = file['file_id']
        # local_path = file['local_path']
        if dataset_id in dataset_dict.keys():
            files = dataset_dict[dataset_id]
            files.append(file)
        else:
            files = [file]
        dataset_dict[dataset_id] = files
    return dataset_dict


def attach_datasets_to_rucio(dataset_id, files, rucio_client, upload_client):

    try:
        dataset = rucio_client.add_dataset(scope=SCOPE, name=dataset_id, rse=RSE)
    except DataIdentifierAlreadyExists as e:
        print('Dataset already exists, skipping...')
    # handling file list, uploading then creating dids to attach to dataset
    dids = []
    for file in files:

        # these lines are commented because they call subprocess instead of rucio python api.

        # try:
        #     # upload file, like physically upload it
        #     # rucio upload --rse DESY-DCACHE --scope <YOUR SCOPE> test-file01
        #     result = subprocess.run(['rucio', 'upload', '--rse', RSE, '--scope', SCOPE, dataset_id])
        #     if result.returncode != 0:
        #         print('Error uploading file: {}'.format(result.stderr))
        # except Exception as e:
        #     print('Error uploading file: {}'.format(e.message))

        # upload client returns 0 if the upload is successful
        # raises :
        # InputValidationError
        # RSEWriteBlocked
        # NoFilesUploaded
        # NotAllFilesUploaded
        # so catching one of the last two at the very least is necessary before continuing

        # still need the file absolute path before trying to upload.
        file_path = os.path.join(constants.datapath_prefix, file['local_path'])
        upload_client(path=file_path, rse=RSE)
        file_dic = {'scope': SCOPE, 'name': file['file_id']}
        dids.append(file_dic)

    attachment = rucio_client.attach_dids(
        scope=SCOPE,
        name=dataset_id,
        dids=dids,
        rse=RSE,
    )


def main():
    """
    Will grab the dataset from esgpull resulted json as well as their contents in files
    Every item in the list will then be attached to the rucio instance.
    """
    print("Retrieving dataset/file dictionary...")
    dataset_dict = get_dataset_list('subset_rucio_cmcc.json')
    print("Init rucio client...")
    rucio_client = Client()
    upload_client = UploadClient()
    print("Processing dictionary items...")
    for key in dataset_dict.keys():
        print('now attaching files from dataset {}...'.format(key))
        attach_datasets_to_rucio(key, dataset_dict[key], rucio_client, upload_client)
        print('Dataset has {} files attached.'.format(len(dataset_dict[key])))
        print('----------------------------------------')

    print('Done!')

if __name__ == '__main__':
    main()

import boto3
import urllib
import json
import logging

s3 = boto3.client('s3')

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.INFO)


def copy_s3_file(originating_bucket, paste_bucket, originating_key, paste_key):
    try:
        logging.debug(f"Attempting to copy s3 object from {originating_bucket}/{originating_key} to {paste_bucket}{paste_key}")
        s3.copy_object(
            Bucket = paste_bucket
            , Key = paste_key
            , CopySource = {
                'Bucket': originating_bucket
                ,'Key': originating_key
                }
            )
    
        logging.info(f"Successfully copied s3 object from {originating_bucket}/{originating_key} to {paste_bucket}{paste_key}!")
    except Exception as e:
        logging.error(e)
        raise(f"Error copying object {originating_key} from bucket {originating_bucket}. Make sure they exist and your bucket is in the same region as this function.")

def lambda_handler(event, context):
    logging.debug(event)
    try:
        event = json.loads(event['Records'][0]['Sns']['Message'])
    except:
        logging.error('Event is not an SNS Message. Unable to Proceed -- Terminating...')
        raise

    originating_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    logging.debug(f"Variables:\noriginating_bucket: {originating_bucket}\nkey: {key}")

    if not key.find("/") == -1:
        file_name = key[key.rfind("/"):]
        paste_bucket = ""
    
        if key.startswith("folder01/nestedfolder/"):
            logging.debug("key starts with ""testingp-01/folder01/nestedfolder/""")

            paste_bucket = "testingp-01"
            paste_key = "folder02"+file_name
        elif key.startswith("folder01/nestedfolder2/"):
            logging.debug("key starts with ""testingp-01/folder01/nestedfolder2/""")

            paste_bucket = "testingp-02"
            paste_key = "folder02/nestedfolder2/"+file_name

        if paste_bucket:
            copy_s3_file(originating_bucket,paste_bucket,key,paste_key)
        else:
            logging.info("No copy needed. Prefix was not within the preset paths.")
    else:
        logging.info("No copy needed. File does not have any prefixes.")
    

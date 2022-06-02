import os
import requests
import json
from google.cloud import storage


bucket = os.getenv("bucket")
bucket_api = os.getenv("bucket_api")

dataprep_recipeId_BS = os.getenv("dataprep_recipeId_BS") 
dataprep_recipeId_IT = os.getenv("dataprep_recipeId_IT") 
dataprep_recipeId_worklog_BS = os.getenv("dataprep_recipeId_worklog_BS") 
dataprep_recipeId_worklog_IT = os.getenv("dataprep_recipeId_worklog_IT") 
dataprep_recipeId_anual = os.getenv("dataprep_recipeId_anual") 
dataprep_recipeId_perfilado = os.getenv("dataprep_recipeId_perfilado") 





def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name): 

  

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name
    )
    source_bucket.delete_blob(blob_name)

    print(
        "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )
 
def delete_blob(bucket_name, blob_name): 

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name) 
    blob = bucket.blob(blob_name)
    blob.delete()

    print("Blob {} deleted.".format(blob_name))



def automataDataprep(event, context):
    

        """Background Cloud Function to be triggered by Cloud Storage.
        Args:
            event (dict): The Cloud Functions event payload.
            context (google.cloud.functions.Context): Metadata of triggering event."""

        

        head_tail = os.path.split(event['name']) 
        newfilename = head_tail[1]
        newfilepath = head_tail[0]

        datataprep_auth_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
      
        print('Recibido fichero en el bucket, dispara CF, ARGS: ' + 'newfilename:' + newfilename +' newfilepath:' + newfilepath)
#############################################################Paso 1 Folder /#############################################################
        
        if context.event_type == 'google.storage.object.finalize' and newfilepath == 'export/BS_RAW_DATA_PARA_IMPUTACIONES': 
           
            if newfilename == 'BS_RAW_DATA_PARA_IMPUTACIONES.csv':
                move_blob(bucket,"export/BS_RAW_DATA_PARA_IMPUTACIONES/BS_RAW_DATA_PARA_IMPUTACIONES.csv",bucket,"export/running/BS_RAW_DATA_PARA_IMPUTACIONES.csv")
            else:
                pass

#############################################################Paso 2 Folder Run#############################################################
        
        if context.event_type == 'google.storage.object.finalize' and newfilepath == 'export/running':
            if newfilename == 'BS_RAW_DATA_PARA_IMPUTACIONES.csv':
                dataprep_jobid = int(dataprep_recipeId_BS) 
                print('Run Dataprep job on new file: {}'.format(newfilename))
                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}}
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
            

            elif newfilename == 'IT_RAW_DATA_PARA_IMPUTACIONES.csv':
                dataprep_jobid = int(dataprep_recipeId_IT) 
                print('Run Dataprep job on new file: {}'.format(newfilename))

                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}}
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
                finished_IT=True

            elif newfilename == 'IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv': 
            
                dataprep_jobid = int(dataprep_recipeId_worklog_BS) 
                print('Run Dataprep job on new file: {}'.format(newfilename))

                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}}
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
                

            elif newfilename == 'IT.json': 
              
             
                dataprep_jobid = int(dataprep_recipeId_worklog_IT)
                print('Run Dataprep job on new file: {}'.format(newfilename))

                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}}
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
               


            elif newfilename == 'DummyAnual.json':
              
                dataprep_jobid = int(dataprep_recipeId_anual)
                print('Run Dataprep job on new file: {}'.format(newfilename))
                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}}
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
              
            elif newfilename == 'DummyPerfilado.json': 
            
                dataprep_jobid = int(dataprep_recipeId_perfilado) 
                print('Run Dataprep job on new file: {}'.format(newfilename))

                dataprep_runjob_endpoint = 'https://api.clouddataprep.com/v4/jobGroups'
                datataprep_job_param = {
                "wrangledDataset": {"id": dataprep_jobid},
                "runParameters": {"overrides": {"data": [{"key": "FileName","value": newfilename}]}} 
                }
                print('Run Dataprep job param: {}'.format(datataprep_job_param))
                dataprep_headers = {
                    "Content-Type":"application/json",
                    "Authorization": "Bearer "+datataprep_auth_token
                }        

                resp = requests.post(
                    url=dataprep_runjob_endpoint,
                    headers=dataprep_headers,
                    data=json.dumps(datataprep_job_param)
                )

                print('Status Code : {}'.format(resp.status_code))      
                print('Result : {}'.format(resp.json()))
               


#############################################################Paso 3 Folder Loaded#############################################################


        if context.event_type == 'google.storage.object.finalize' and newfilepath == 'export/loaded':
            if newfilename == 'BS_RAW_DATA_PARA_IMPUTACIONES.csv':
                
                move_blob(bucket_api,"export/IT_RAW_DATA_PARA_IMPUTACIONES/IT_RAW_DATA_PARA_IMPUTACIONES.csv",bucket,"export/running/IT_RAW_DATA_PARA_IMPUTACIONES.csv")
                
            if newfilename == 'IT_RAW_DATA_PARA_IMPUTACIONES.csv':
                
                move_blob(bucket_api,"export/IMPUTACIONES_XXX_BS_IT/IMPUTACIONES_XXX_BS_IT_WORKLOG_ID_1.csv",bucket,"export/running/IMPUTACIONES_XXX_BS_IT_WORKLOG_ID_1.csv")
                move_blob(bucket_api,"export/IMPUTACIONES_XXX_BS_IT/IMPUTACIONES_XXX_BS_IT_ISSUE_ID_2.csv",bucket,"export/running/IMPUTACIONES_XXX_BS_IT_ISSUE_ID_2.csv")
                move_blob(bucket_api,"export/IMPUTACIONES_XXX_BS_IT/IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv",bucket,"export/running/IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv")

               
            if newfilename == 'IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv':
                move_blob(bucket,"export/dummys/IT.json",bucket,"export/running/IT.json")
               
            if newfilename == 'IT.json': 
                move_blob(bucket,"export/dummys/DummyAnual.json",bucket,"export/running/DummyAnual.json")
            
            if newfilename == 'DummyAnual.json':
                move_blob(bucket,"export/dummys/DummyPerfilado.json",bucket,"export/running/DummyPerfilado.json") 
               
            if newfilename == 'DummyPerfilado.json': 

             
                
                delete_blob(bucket, 'export/running/BS_RAW_DATA_PARA_IMPUTACIONES.csv')
                delete_blob(bucket, 'export/running/IT_RAW_DATA_PARA_IMPUTACIONES.csv') 
                delete_blob(bucket, 'export/running/IMPUTACIONES_XXX_BS_IT_WORKLOG_ID_1.csv')
                delete_blob(bucket, 'export/running/IMPUTACIONES_XXX_BS_IT_ISSUE_ID_2.csv')
                delete_blob(bucket, 'export/running/IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv')
                move_blob(bucket,"export/running/IT.json",bucket,"export/dummys/IT.json") 
                move_blob(bucket,"export/running/DummyAnual.json",bucket,"export/dummys/DummyAnual.json") 
                move_blob(bucket,"export/loaded/DummyPerfilado.json",bucket,"export/dummys/DummyPerfilado.json") 
                delete_blob(bucket, 'export/running/DummyPerfilado.json') 
                delete_blob(bucket, 'export/loaded/BS_RAW_DATA_PARA_IMPUTACIONES.csv')
                delete_blob(bucket, 'export/loaded/IT_RAW_DATA_PARA_IMPUTACIONES.csv') 
                delete_blob(bucket, 'export/loaded/IMPUTACIONES_XXX_BS_IT_ISSUE_KEY_3.csv')
                delete_blob(bucket, 'export/loaded/IT.json') 
                delete_blob(bucket, 'export/loaded/DummyAnual.json') 
           
            

        return 'End File event'.format(newfilename)

if __name__ == '__main__':
    automataDataprep(None,None)

        

    
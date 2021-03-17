############################################################################
## Lambda Funktion - Volllaufschutz S3
############################################################################
## Der Volllaufschutz soll überwachen ob ein S3 Bucket über eine bestimmte 
## Größe anwächst. Als Reaktion kann man definieren ob man die Files bei 
## Überlauf in ein neues Bucket kopiert und ob man eine Benachrichtigung 
## bekommt. 
############################################################################
## Author: Kevin Lempert
## Copyright: Copyright 2021, CC3
## Version: 1.0.5
## Status: prod
############################################################################
## Env-Var:
##      ev_copy	            bool        Soll Datei kopiert werden 
##      ev_message	        bool        Soll E-Mail gesendet werden
##      ev_reserve_bucket	string      Zielbucket Kopieren
##      ev_size_max	        float       Max Größe Zahl
##      ev_size_value	    string      Max Größe Einheit (B/KB/MB/GB)
############################################################################

import json
import urllib.parse
import boto3
import os 

s3 = boto3.resource('s3')


# Check if the bucket is full	
def bucket_is_full(byte):
    size_value = os.environ['ev_size_value']
    max = float(os.environ['ev_size_max'])
    if(size_value == "B"):
        return byte > max
    elif(size_value == "KB"):
        return byte > (max * 1024)
    elif(size_value == "MB"):
        return byte > (max * 1024 * 1024)
    elif(size_value == "GB"):
        return byte > (max * 1024 * 1024 * 1024)
    else:
        print("Wrong Size Value (B/KB/MB/GB)")
        exit()


def lambda_handler(event, context):
    
    # get information from Trigger
    main_bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    bucket = s3.Bucket(main_bucket_name)

    try:
        print("Bucket Exist! Start...")

        # Calculate max Size 
        total_size = 0
        for k in bucket.objects.all():
            total_size += k.size
        print("total_size: " + str( total_size ))


        #Test if Bucket is full 
        if(bucket_is_full(total_size)):
            #Bucket is full
            print("Bucket " + main_bucket_name + " is full!")
            
            # Copy if env-var is true 
            if(os.environ['ev_copy'].lower()  == "true"):
                print("Start File Copy!")
                try:
                    reserve_bucket = os.environ['ev_reserve_bucket']
                    bucket_des = s3.Bucket(reserve_bucket)
                    
                    copy_source = {
                        'Bucket': main_bucket_name,
                        'Key': file_key
                    }
                    print(copy_source)
                    
                    bucket_des.copy(copy_source, file_key)
                    print("File Copy complete!")
                    
                    s3.Object(main_bucket_name, file_key).delete()
                    print("File delete complete!")
                    
                except Exception as e:
                    print(e)
                    raise e
            else:
                print("File Copy is false!")
                
            # Message
            if(os.environ['ev_message'].lower()  == "true"):
                sns_client = boto3.client('sns')
                
                des_bucket = ''
                if(os.environ['ev_copy'].lower()  == "true"):
                    des_bucket = os.environ['ev_reserve_bucket']

                response = sns_client.publish(
                    TargetArn='arn:aws:sns:us-east-1:414909593523:Volllaufschutz',
                    Subject='Volllaufschutz aktiviert',
                    Message='Der Bucket ' + main_bucket_name + ' ist vollgelaufen! \n\n' +
                            'Schutz-Groeße: ' + os.environ['ev_size_max'] +' '+ os.environ['ev_size_value'] + '\n' + 
                            'Datei: ' + file_key + '\n' + 
                            'Kopieren der Dateien: ' + os.environ['ev_copy'] + '\n' +
                            'Zielbucket (wenn Kopiervorgang gestartet wurde): ' + des_bucket,
                    MessageStructure='txt'
                )
            return {'msg' : 'Volllaufschutz erfolgreich ausgeführt'}
        else:
            print("Bucket has enough space!")
            return {'msg' : 'Bucket hat noch Platz'}

    except Exception as e:
        print(e)
        raise e

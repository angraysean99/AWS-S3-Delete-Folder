import csv
import boto3
from datetime import datetime
import os
import sys
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bucket", help="name of bucket")
    parser.add_argument("file", help="name of file with ID")

    args = parser.parse_args()


access_key = ''
secret_access_key = ''


dirname = os.getcwd()           #gets information from credential.txt
filecredit = os.path.join(dirname,'credential.txt')


with open(filecredit, 'r') as f:
    lines = f.readlines()
access_key = str(lines[0]).strip()
secret_access_key = str(lines[1]).strip()


s3_client = boto3.client('s3',
                      aws_access_key_id=access_key,
                      aws_secret_access_key=secret_access_key)
s3_resource = boto3.resource('s3',
                           aws_access_key_id = access_key,
                           aws_secret_access_key = secret_access_key)


bucketName = args.bucket            #checks if bucket is valid
if s3_resource.Bucket(bucketName).creation_date is None:
    print("Invalid bucket name")
    sys.exit()


dellist = []

filecsv = args.file
if not ".csv" in filecsv:
    filecsv += ".csv"
dirname = os.getcwd()
filename = os.path.join(dirname,filecsv)
try:                                    #checks if the file name with ID is valid
    with open(filecsv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            dellist.append(row[0])    #list of files to be deleted from bucket
except FileNotFoundError:
    print("Invalid file name")
    sys.exit()
except IndexError:
    print("File is empty.")
    sys.exit()

now = datetime.now()     #finds the datetime and sets a new row in log to separate previous entry
dt_string = now.strftime("%d/%m/%Y-%H:%M:%S,")


#getting list of all the keys in bucket
keys = []
resp = s3_client.list_objects_v2(Bucket=bucketName)

if (resp['KeyCount'] == 0):
    print("Bucket is empty.")
    sys.exit()
else:
    for obj in resp['Contents']:
        keys.append(obj['Key'])


#want to create a text file
try:                        ##checks for if file exist (does the log file need to contain past info?)
    f = open("logs.txt", 'w')
    f.write(dt_string + '\n')
    f.write("Name_Of_Obj,Status\n")
except FileNotFoundError:
    f = open("logs.txt", 'w')
    f.write(dt_string + '\n')
    f.write("Name_Of_Obj,Status\n")

for j in range(len(dellist)):

    objectName = str(dellist[j])     #check if key to delete is in bucket
    folder = objectName + '/'
    check = len(folder)
    if (folder in keys):     #checks if it is folder
        bucket = s3_resource.Bucket(bucketName)
        f = open("logs.txt","a")
        f.write(objectName + ',DELETED\n')
        innerobj = bucket.objects.filter(Prefix = (objectName + '/'))
        for innerobj in innerobj:               #gets the name of the objects inside folder
            yy = innerobj.key.replace(objectName + '/', "", 1)
            if (yy != ''):
                f.write(yy + ',-\n')
        f.close()
        bucket.objects.filter(Prefix = (objectName + '/')).delete()


    else:           #if object(folder) is not found in the keys
        f = open("logs.txt","a")
        f.write(objectName + ',OBJECT_NOT_FOUND\n')
        f.close()


with open('logs.txt', 'r') as in_file:      #converts txt to csv file
    stripped = (line.strip() for line in in_file)
    lines = (line.split(",") for line in stripped if line)
    with open('logs.csv', 'w', newline = '') as out_file:
        writer = csv.writer(out_file)
        writer.writerows(lines)

# with open("logs.csv") as fp:
#     mytable = from_csv(fp,delimiter = ',')
# print(mytable)

print("Program has finished running. Please check the logs.txt file for the status of objects in bucket")
sys.exit()

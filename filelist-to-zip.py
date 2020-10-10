#!/usr/bin/env python3

import json
import os
import onevizion
import shutil
from zipfile import ZipFile
import urllib

# Read settings
with open('settings','r') as p:
	params = json.loads(p.read())

try:
	OvUserName = params['OV']['UserName']
	OvPassword = params['OV']['Password']
	OvUrl      = params['OV']['Url']
	TrackorType = params['TrackorType']
	ReadyStatus = params['ReadyStatus']
	BlobIdList    = params['BlobIdList']
	ZipFieldName  = params['ZipFieldName']
	ZipFieldFileName = params['ZipFieldFileName']
	ZipErrorField    = params['ZipErrorField']
except Exception as e:
	raise "Please check settings"

if ZipFieldFileName is None or ZipFieldFileName == "":
	fields = [ReadyStatus,BlobIdList,'TRACKOR_KEY']
else:
	fields = [ReadyStatus,BlobIdList,ZipFieldFileName,'TRACKOR_KEY']

# make sure api user has RE on the tab with checkbox and the field list of blobs and RE for the trackor type(sometimes Checklist) and R for WEB_SERVICES 
Req = onevizion.Trackor(trackorType = TrackorType, URL = OvUrl, userName=OvUserName, password=OvPassword)
Req.read(filters = {ReadyStatus:'Queued'}, 
		fields = fields, 
		sort = {'TRACKOR_KEY':'ASC'}, page = 1, perPage = 1000)

if len(Req.errors)>0:
	# If can not read list of efiles then must be upgrade or something.  Quit and try again later.
	print(Req.errors)
	quit(1)

print("Found {x} records".format(x=len(Reg.jsonData)))
for cl in Req.jsonData:
	print('Starting '+cl['TRACKOR_KEY'])
	try:
		if cl[ZipFieldFileName] is None or cl[ZipFieldFileName] == "":
			zipFileName = cl['TRACKOR_KEY']+'.zip'
		else:
			zipFileName = cl[ZipFieldFileName]+'.zip'
	except Exception as e:
		zipFileName = cl['TRACKOR_KEY']+'.zip'

	hasErrors = False
	errors = {}

	print(cl[BlobIdList])
	if cl[BlobIdList] is None:
		#todo add error handling
		print('no file list for '+cl['TRACKOR_KEY'])
		hasErrors = True
		errors = 'no file list for '+cl['TRACKOR_KEY']
		updateFields = {}
		updateFields[ReadyStatus] = "Error"
		updateFields[ZipErrorField] = errors
		Req.update(filters = {'TRACKOR_ID': cl['TRACKOR_ID']}, fields = updateFields)
		continue

	with ZipFile(zipFileName,'w') as zipObj:

		for f in cl[BlobIdList].split('\n') :

			TrackorID, FieldName, BlobId, FilePath, FileName = f.split('|')

			EFileReq = onevizion.Trackor(URL = OvUrl, userName=OvUserName, password=OvPassword)
			tmpFileName = EFileReq.GetFile(trackorId=TrackorID, fieldName=FieldName)

			#todo add error handling
			if len(EFileReq.errors)>0:
				hasErrors - True
				errors = EFileReq.errors

			# Do Local File copy
			if not os.path.exists(FilePath):
				os.makedirs(FilePath)
			try:
				shutil.move(tmpFileName,FilePath+'/'+FileName)
			except Exception as e:
				print (e)

			print(tmpFileName+'|'+FilePath+'/'+FileName)

			zipObj.write(FilePath+'/'+FileName)
			os.remove(FilePath+'/'+FileName)

	if hasErrors:
		updateFields = {}
		updateFields[ReadyStatus] = "Error"
		updateFields[ZipErrorField] = errors
		Req.update(filters = {'TRACKOR_ID': cl['TRACKOR_ID']}, fields = updateFields)
		os.remove(zipFileName)
	else:
		updateFields = {}
		updateFields[ZipFieldName] = onevizion.EFileEncode(zipFileName)
		updateFields[ZipErrorField] = ""
		updateFields[ReadyStatus] = "Completed"
		Req.update(filters = {'TRACKOR_ID': cl['TRACKOR_ID']}, fields = updateFields)
		os.remove(zipFileName)


from datetime import datetime
import sys
import os
import xml.etree.ElementTree as ET 
import configparser
import zipfile

#from pymysql import NULL

#from classes import dbhelper
from classes import mdbhelper

config = configparser.ConfigParser()
config.read("smp.ini")


print('Initialization engine...')
#db = dbhelper.DBHelper()
db = mdbhelper.MDBHelper()
credentials = config['DBSettings']
conn_result = db.init(credentials)
print(f'Database connection result {conn_result}')
print('... initialized')

#sys.exit('Connected to DB')

# Functions 
def convDate(str_date):
  docDate = datetime.strptime(str_date, "%d.%m.%Y")
  dbDate = docDate.strftime("%Y-%m-%d")
  return dbDate

def createOrg(tag):  
  patronymic = None
  orgShortName = None
  if (tag.find('ИПВклМСП')):
    if ('Отчетство' in tag.find('ИПВклМСП/ФИОИП').attrib):
      patronymic = tag.find('ИПВклМСП/ФИОИП').attrib['Отчество']
  if (tag.attrib['ВидСубМСП'] == '1'):
    if ('НаимОргСокр' in tag.find('ОргВклМСП').attrib):
      orgShortName = tag.find('ОргВклМСП').attrib['НаимОргСокр']

  rowdata = {
    'fullname': tag.find('ОргВклМСП').attrib['НаимОрг'] if (tag.attrib['ВидСубМСП'] == '1') else None,    
    #'shortname': tag.find('ОргВклМСП').attrib['НаимОргСокр'] if (tag.attrib['ВидСубМСП'] == '1') else None,
    'shortname': orgShortName,
    'first_name': tag.find('ИПВклМСП/ФИОИП').attrib['Фамилия'] if tag.find('ИПВклМСП/ФИОИП') else None,
    'last_name': tag.find('ИПВклМСП/ФИОИП').attrib['Имя'] if tag.find('ИПВклМСП/ФИОИП') else None,
    #'mid_name': tag.find('ИПВклМСП/ФИОИП').attrib['Отчество'] if tag.find('ИПВклМСП') else None,    
    'mid_name': patronymic,
    'inn': tag.find('ОргВклМСП').attrib['ИННЮЛ'] if (tag.attrib['ВидСубМСП'] == '1') else tag.find('ИПВклМСП').attrib['ИННФЛ'],
    'ogrn': tag.find('ОргВклМСП').attrib['ОГРН'] if (tag.attrib['ВидСубМСП'] == '1') else tag.find('ИПВклМСП').attrib['ОГРНИП'],
    'kpp': None,
    #'kpp': tag.find('ОргВклМСП').attrib['КПП'] if ('КПП' in tag.find('ОргВклМСП').attrib ) else NULL if (tag.attrib['ВидСубМСП'] == '1') else tag.find('ИПВклМСП').attrib['КПП'] if ('КПП' in tag.find('ИПВклМСП').attrib ) else NULL,
    'created':  convDate(tag.attrib['ДатаВклМСП']),
    'doc_guid':  tag.attrib['ИдДок'],  
    'cat_id':  int(tag.attrib['КатСубМСП']),
    'type_id': int(tag.attrib['ВидСубМСП']),
    'work_cnt': int(tag.attrib['ССЧР']) if ('ССЧР' in tag.attrib) else 0,
    'is_social': tag.attrib['СведСоцПред'] == '1',
    'is_new': tag.attrib['ПризНовМСП'] == '1'
  }
  return rowdata

def createPosition(tag):    
  regcode = tag.find('СведМН').attrib['КодРегион']
  rowdata = []
  for addr in tag.findall('СведМН/*'):
    addrRow = {
      'regCode': int(regcode),
      'posType': addr.attrib['Тип'],
      'posName': addr.attrib['Наим']
    }
    rowdata.append(addrRow)
  return rowdata

def createOKVED(tag):  
  rowdata = []
  for row in tag.findall('СвОКВЭД/*'):
    okvedRow = {
      'isMain': row.tag == 'СвОКВЭДОсн',
      'okvedCode': row.attrib['КодОКВЭД'],
      'okvedName': row.attrib['НаимОКВЭД'] if ('НаимОКВЭД' in row.attrib) else None,
      'okvedVer': row.attrib['ВерсОКВЭД'],
    }
    rowdata.append(okvedRow)
  return rowdata

def createProducts(tag):  
  rowdata = []
  for row in tag.findall('СвПрод'):
    prodRow = {
      'prodCode': row.attrib['КодПрод'],
      'prodName': row.attrib['НаимПрод'],
      'isHighTech': row.attrib['ПрОтнПрод'] == '1'
    }
    rowdata.append(prodRow)
  return rowdata

def createPartners(tag):  
  rowdata = []
  for row in tag.findall('СвПрогПарт'):
    docDate = datetime.strptime(row.attrib['ДатаДог'], "%d.%m.%Y")
    dbDate = docDate.strftime("%Y-%m-%d")
    partnerRow = {
      'partnerName': row.attrib['НаимЮЛ_ПП'],
      'partnerINN': row.attrib['ИННЮЛ_ПП'],
      'partnerAgreementNo': row.attrib['НомДог'],
      'partnerAgreementDate': convDate(row.attrib['ДатаДог'])
    }
    rowdata.append(partnerRow)
  return rowdata

def create44FZ(tag):  
  rowdata = []
  for row in tag.findall('СвКонтр'):
    partnerRow = {
      'contrClient': row.attrib['НаимЮЛ_ЗК'],
      'contrClientINN': row.attrib['ИННЮЛ_ЗК'],
      'contrSubj': row.attrib['ПредмКонтр'] if ('ПредмКонтр' in row.attrib) else None,
      'contrNum': row.attrib['НомКонтрРеестр'],
      'contrDate': convDate(row.attrib['ДатаКонтр']) if ('ДатаКонтр' in row.attrib) else None
    }
    rowdata.append(partnerRow)
  return rowdata

def create223FZ(tag):  
  rowdata = []
  for row in tag.findall('СвДог'):
    partnerRow = {
      'agrClient': row.attrib['НаимЮЛ_ЗД'],
      'agrClientINN': row.attrib['ИННЮЛ_ЗД'],
      'agrSubj': row.attrib['ПредмДог'] if ('ПредмДог' in row.attrib) else None,
      'agrNum': row.attrib['НомДогРеестр'],
      'agrDate': convDate(row.attrib['ДатаДог'])  if ('ДатаДог' in row.attrib) else None
    }
    rowdata.append(partnerRow)
  return rowdata


def createLicense(tag):  
  rowdata = []
  for row in tag.findall('СвЛиценз'):
    licRow = {
      'licNum': row.attrib['НомЛиценз'],
      'licSerial': row.attrib['СерЛиценз'] if ('СерЛиценз' in row.attrib) else None,
      'licIssued': convDate(row.attrib['ДатаЛиценз']) if ('ДатаЛиценз' in row.attrib) else '0000-00-00',
      'licStart': convDate(row.attrib['ДатаНачЛиценз']) if ('ДатаНачЛиценз' in row.attrib) else '0000-00-00',
      'licIssuedBy': row.attrib['ОргВыдЛиценз'] if ('ОргВыдЛиценз' in row.attrib) else None,      
      'licEnd': convDate(row.attrib['ДатаКонЛиценз']) if ('ДатаКонЛиценз' in row.attrib) else None,
      'licRevoked': convDate(row.attrib['ДатаОстЛиценз']) if ('ДатаОстЛиценз' in row.attrib) else None,
      'licRevokedBy': row.attrib['ОргОстЛиценз'] if ('ОргОстЛиценз' in row.attrib) else None,
      'licIssuer': row.attrib['ОргВыдЛиценз'] if ('ОргВыдЛиценз' in row.attrib) else None,
      'licType': row.attrib['ВидЛиценз'] if ('ВидЛиценз' in row.attrib) else None,
      'licActivity':''
    }
    for act in row.findall('НаимЛицВД'):
      licRow['licActivity'] = act.text
    rowdata.append(licRow)
  return rowdata

def processFile():
  print(f"Parsing XML file...")
  eltree = ET.parse('data.xml')
  print(f"XML file parsed, looking for root node...")
  root_node = eltree.getroot() 
  print(f"Root node found, working with data")
  rec_cnt = 0  
  for tag in root_node.findall('Документ'):    
    org = createOrg(tag)    
    rec_cnt = rec_cnt + 1    
    orgResult = db.addOrg(org)    
    if (orgResult[2] == 'ok'):      
      orgID = orgResult[0]      
      pos = createPosition(tag)      
      for line in pos:
        res = db.addAddressLine(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddAddressLine: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New address line added for organisation id {orgID}")
      

      okved = createOKVED(tag)      
      for line in okved:        
        res = db.addOrgOKVED(orgID, line)        
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrgOKVED: {res[1]}, OKVED code {res[0]}")
        # if (res[1] == 'new'):
        #   print(f"New OKVED added for organisation id {orgID}")      

      lic = createLicense(tag)      
      for line in lic:
        res = db.addOrgLic(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrgLic: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New License added for organisation id {orgID}")         

      partn = createPartners(tag)      
      for line in partn:
        res = db.addOrgPartner(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrgPartner: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New Partner added for organisation id {orgID}")      

      prod = createProducts(tag)      
      for line in prod:
        res = db.addOrgProduct(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrgProduct: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New Product added for organisation id {orgID}")      

      FZ44 = create44FZ(tag)      
      for line in FZ44:
        res = db.addOrg44FZ(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrg44FZ: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New 44FZ contract added for organisation id {orgID}")      

      FZ223 = create223FZ(tag)      
      for line in FZ223:
        res = db.addOrg223FZ(orgID, line)
        if (res[2] != 'ok'):        
          sys.exit(f"Abnormal termination of AddOrg223FZ: {res[1]}")
        # if (res[1] == 'new'):
        #   print(f"New 223FZ contract added for organisation id {orgID}")      
    else:
      sys.exit(f"Organisation creation error {orgResult[1]}, termination")
  print(f'File processing completed, {rec_cnt} records processed')
  return rec_cnt
    

print(f"Reading ZIP file...")
zip_archive = zipfile.ZipFile("rawData\\data-10012022-structure-10082021.zip", "r")
filedata = zip_archive.infolist()
print(f"ZIP file readed, processing:")
cnt = 0
if (os.path.isfile('data.xml')):
  processFile()
  os.remove('data.xml')
  cnt = cnt+1

print(f"Previous tail processed, continue:")

for file_info in filedata:  
  print(f"Extracting datafile {file_info.filename}, size {file_info.file_size} bytes...")  
  zip_archive.extract(file_info.filename)
  os.rename(file_info.filename, 'data.xml') 
  freg_result = db.registerFile(file_info.filename)
  rec_count = 0
  if (freg_result[1] == 'new'):
    try:
      rec_count = processFile()
    except Exception as e:
      print(f'Exception occured processing file: {e}, exit')
      sys.exit('Bye!')
    db.updFile({'fileName': file_info.filename, 'rowCount': rec_count})
    cnt = cnt + 1
  elif (freg_result[1] == 'exists'):    
    print(f'File {file_info.filename} was processed before, skip ')
  else:
    print(f'File {file_info.filename} registering error, skip ')
  os.remove('data.xml')  
  print(f'Now processed {cnt} files ')
  if (cnt > 500 ):
    sys.exit('More than 100 files processed, control needed')     

db.disconnect()
sys.exit('Archive processed tested')
  
#
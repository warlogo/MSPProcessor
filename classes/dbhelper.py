import pymysql

class DBHelper:
  __dbCred = None  
  def init(self, credentials):
    #self.__con = pymysql.connect(  credentials['host'],credentials['user'],credentials['password'],credentials['basename'],credentials['port'],charset='utf8mb4')
    self.__dbCred = credentials
    conn_res = self.connect()
    if not conn_res:
      print('DBHelper: init error')
      self.__dbCred = None
      return False
    else:
      conn_res.execute("SELECT VERSION()")
      version = conn_res.fetchone()
      print(f"Database version: {version[0]}")
      return True    

  def connect(self):
    if not self.__dbCred:
      print('DBHelper class: Not inited')
      return False
    try:
      conn = pymysql.connect(host=self.__dbCred['host'],user=self.__dbCred['user'],password=self.__dbCred['password'],db=self.__dbCred['basename'],charset=self.__dbCred['charset'])
    except Exception as e:
      print(f'Database connecting error {e}')      
      return False    
    return conn.cursor()      

  def addOrg(self, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [
      rowdata['fullname'], rowdata['shortname'], rowdata['first_name'], rowdata['last_name'],  rowdata['mid_name'],  rowdata['inn'], rowdata['ogrn'], rowdata['kpp'],
      rowdata['created'], rowdata['doc_guid'],  rowdata['cat_id'],  rowdata['type_id'],  rowdata['work_cnt'], 
      rowdata['is_social'], rowdata['is_new']
    ]
    conn.callproc('addOrg', payload)
    for result in conn.fetchall():        
        ret = result
    return ret

  def addAddressLine(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [
      orgID, rowdata['regCode'], rowdata['posType'], rowdata['posName']
    ]    
    conn.callproc('addOrgAddressLine', payload)
    for result in conn.fetchall():        
        ret = result
    return ret

  def addOrgOKVED(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [
      orgID, rowdata['okvedCode'], rowdata['okvedName'], rowdata['okvedVer'], rowdata['isMain']
    ]
    conn.callproc('addOrgOKVED', payload)
    for result in conn.fetchall():        
        ret = result
    return ret

  def addOrgLic(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      orgID, rowdata['licSerial'], rowdata['licNum'], rowdata['licType'], rowdata['licActivity'],  rowdata['licIssued'],  rowdata['licIssuedBy'], rowdata['licStart'],
      rowdata['licEnd'],  rowdata['licRevoked'],  rowdata['licRevokedBy']
    ]
    conn.callproc('addOrgLicense', payload)
    for result in conn.fetchall():        
        ret = result
    return ret

  def addOrgPartner(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      orgID, rowdata['partnerName'], rowdata['partnerINN'], rowdata['partnerAgreementNo'], rowdata['partnerAgreementDate']
    ]
    conn.callproc('addOrgPartner', payload)
    for result in conn.fetchall():        
        ret = result
    return ret
  
  def addOrgProduct(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      orgID, rowdata['prodCode'], rowdata['prodName'], rowdata['isHighTech']
    ]
    conn.callproc('addOrgProduct', payload)
    for result in conn.fetchall():        
        ret = result
    return ret
  
  def addOrg44FZ(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      orgID, rowdata['contrClient'], rowdata['contrClientINN'], rowdata['contrSubj'], rowdata['contrNum'], rowdata['contrDate']
    ]
    conn.callproc('addOrg44FZ', payload)
    for result in conn.fetchall():        
        ret = result
    return ret
  
  def addOrg223FZ(self, orgID, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      orgID, rowdata['agrClient'], rowdata['agrClientINN'], rowdata['agrSubj'], rowdata['agrNum'], rowdata['agrDate']
    ]
    conn.callproc('addOrg223FZ', payload)
    for result in conn.fetchall():        
        ret = result
    return ret
  
  def registerFile(self, filename):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      filename
    ]
    conn.callproc('registerFile', payload)
    for result in conn.fetchall():        
        ret = result
    return ret
  
  def updFile(self, rowdata):
    conn = self.connect()
    if not conn:
      return False
    payload = [    
      rowdata['fileName'], rowdata['rowCount']
    ]
    conn.callproc('updateFileLog', payload)
    print('file updated')
    for result in conn.fetchall():
        print('some return')
        ret = result
    return ret
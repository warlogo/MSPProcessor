import mysql.connector

class MDBHelper:
  __dbCred = None
  __dbConn = None
  
  def init(self, credentials):
    #self.__con = pymysql.connect(  credentials['host'],credentials['user'],credentials['password'],credentials['basename'],credentials['port'],charset='utf8mb4')
    self.__dbCred = credentials
    conn_res = self.connect()
    if not conn_res:
      print('DBHelper: init error')
      self.__dbCred = None
      self.__dbConn = None
      return False
    else:      
      curs = conn_res.cursor()
      curs.execute("SELECT VERSION()")
      version = curs.fetchone()
      print(f"Database version: {version[0]}")
      curs.close()
      return True
  
  def isConnected(self):    
    if (self.__dbConn == None ):      
      return False
    try:      
      self.__dbConn.ping()
    except:      
      return False    
    return True

  def connect(self):
    if not self.__dbCred:
      print('DBHelper class: Not inited')
      return False
    if (self.isConnected()):
      return self.__dbConn
    try:
      conn = mysql.connector.connect(
        host=self.__dbCred['host'],
        user=self.__dbCred['user'],
        password=self.__dbCred['password'],
        db=self.__dbCred['basename'], 
        autocommit=True, 
        charset=self.__dbCred['charset'],
        collation='utf8mb4_general_ci'
      )               
      print(conn.autocommit)      
      print(conn.charset)
    except Exception as e:
      print(f'Database connecting error {e}')      
      return False    
    self.__dbConn = conn
    return conn

  def disconnect(self):    
    try:      
      self.__dbConn.close()
    except Exception as e:
      print(f'Database connecting error {e}')      
      return False    
    return True

  def retResult(self, cursor):
    ret = []
    for result in cursor.stored_results():        
        ret = result.fetchone()
    return ret
  
  def callMSProc(self, procName, payload):
    if (not self.isConnected()):
      self.connect()
      if (not self.isConnected()):
        print(f'Connection lost, giving up...')
        return ['-5', 'Connection lost', 'error']
    cur = self.__dbConn.cursor()
    try:      
      cur.callproc(procName, payload)
    except Exception as e:
      print(f'SP {procName} execution error {e}')      
      return ['-5', e, 'error']    
    res = self.retResult(cur)    
    cur.close()    
    return res

  def addOrg(self, rowdata):    
    payload = [
      rowdata['fullname'], rowdata['shortname'], rowdata['first_name'], rowdata['last_name'],  rowdata['mid_name'],  rowdata['inn'], rowdata['ogrn'], rowdata['kpp'],
      rowdata['created'], rowdata['doc_guid'],  rowdata['cat_id'],  rowdata['type_id'],  rowdata['work_cnt'], 
      rowdata['is_social'], rowdata['is_new']
    ]        
    return self.callMSProc('addOrg', payload)

  def addAddressLine(self, orgID, rowdata):    
    payload = [
      orgID, rowdata['regCode'], rowdata['posType'], rowdata['posName']
    ]
    return self.callMSProc('addOrgAddressLine', payload)

  def addOrgOKVED(self, orgID, rowdata):    
    payload = [
      orgID, rowdata['okvedCode'], rowdata['okvedName'], rowdata['okvedVer'], rowdata['isMain']
    ]    
    return self.callMSProc('addOrgOKVED', payload)

  def addOrgLic(self, orgID, rowdata):    
    payload = [    
      orgID, rowdata['licSerial'], rowdata['licNum'], rowdata['licType'], rowdata['licActivity'],  rowdata['licIssued'],  rowdata['licIssuedBy'], rowdata['licStart'],
      rowdata['licEnd'],  rowdata['licRevoked'],  rowdata['licRevokedBy']
    ]    
    return self.callMSProc('addOrgLicense', payload)
    

  def addOrgPartner(self, orgID, rowdata):    
    payload = [    
      orgID, rowdata['partnerName'], rowdata['partnerINN'], rowdata['partnerAgreementNo'], rowdata['partnerAgreementDate']
    ]  
    return self.callMSProc('addOrgPartner', payload)
  
  def addOrgProduct(self, orgID, rowdata):    
    payload = [    
      orgID, rowdata['prodCode'], rowdata['prodName'], rowdata['isHighTech']
    ]    
    return self.callMSProc('addOrgProduct', payload)
  
  def addOrg44FZ(self, orgID, rowdata):    
    payload = [    
      orgID, rowdata['contrClient'], rowdata['contrClientINN'], rowdata['contrSubj'], rowdata['contrNum'], rowdata['contrDate']
    ]    
    return self.callMSProc('addOrg44FZ', payload)
  
  def addOrg223FZ(self, orgID, rowdata):    
    payload = [    
      orgID, rowdata['agrClient'], rowdata['agrClientINN'], rowdata['agrSubj'], rowdata['agrNum'], rowdata['agrDate']
    ]    
    return self.callMSProc('addOrg223FZ', payload)
  
  def registerFile(self, filename):    
    payload = [    
      filename
    ]    
    return self.callMSProc('registerFile', payload)
  
  def updFile(self, rowdata):    
    payload = [    
      rowdata['fileName'], rowdata['rowCount']
    ]    
    return self.callMSProc('updateFileLog', payload)
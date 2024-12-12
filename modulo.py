import json
import requests
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

campaignId = 'xxxxxxxxxxxxxxxx'

def getBearerToken():
    oauthUrl = 'https://login.microsoftonline.com/xxxxxxxxxx/oauth2/token'
    

    params = {
        'client_id' : 'xxxxxxxxxxxxxxxxxxx',
        'client_secret' : 'xxxxxxxxxxxxxxxx',
        'resource' : 'https://xxxxxcloud.crm2.dynamics.com/',
        'grant_type' : 'client_credentials'
    }

    resp = requests.post(oauthUrl, data=params)

    if resp.status_code != 200:
        errorMessage = 'Error: ' + str(resp.status_code)
        return errorMessage
    else:
        bearer_token = (resp.json())['access_token']
        return bearer_token

tokenOauth = getBearerToken()

apiUrl = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0"

accountId = ''

crmrequestheaders = {
    'Authorization': 'Bearer ' + tokenOauth,
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=utf-8',
    'Prefer': 'return=representation'
}

# Hago una query vacia para ver si el tocken esta bien
tokenRes = requests.get(apiUrl, headers=crmrequestheaders)

# En este if tambien podria poner tokenRes.ok
if tokenRes.status_code  != 200:
    # Handle any missing key errors
    print('Could not get access token')
    exit()

# Traigo la variable que me dice si la campaña es una reunion grupal y para obtener el nombre de la campaña
queryUrlCampaignReunion = "/campaigns?$select=aw_reuniongrupal,name&$filter=campaignid eq " + campaignId + ""
resCampaignReunion = requests.get(apiUrl+queryUrlCampaignReunion, headers=crmrequestheaders)
reunionGrupal = resCampaignReunion.json()['value'][0]['aw_reuniongrupal']
campaignName = resCampaignReunion.json()['value'][0]['name']

def createContact(token,userFirstname,userLastname,userEmail,idAccount,userJobtitle):
    # Vuelvo a agregar los caracteres que habia sacado para que no rompa la query 
    userFirstname = str(userFirstname).replace("%60", "´")
    userFirstname = str(userFirstname).replace("%60", "'")
    userLastname = str(userLastname).replace("%60", "´")
    userLastname = str(userLastname).replace("%60", "'")
    userJobtitle = str(userJobtitle).replace("%26", "&")

    requestUrl = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0/contacts"

    requestHeaders = {
        'Authorization': 'Bearer ' + token,
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=utf-8',
        'Prefer': 'return=representation'
    }

    data =  {
        'parentcustomerid_account@odata.bind' : "/accounts(" + idAccount + ")",
        'firstname' : userFirstname,
        'lastname' : userLastname,
        'emailaddress1' : userEmail,
        'aw_departamentolaboralid@odata.bind': '/aw_departamentos(8F2AB685-0D7B-E811-8D1D-005056880CBF)',
        'jobtitle': userJobtitle
    }

    #Aca se crea el contacto
    resContact = requests.post(requestUrl, headers=requestHeaders, data=json.dumps(data))

    if str(resContact) == '<Response [412]>':
        return 'Error 412'
    else:    
        #Esto es para usarlo en inserHit
        resContact = resContact.json()
        return resContact['contactid']


def activateHit(token, assistId):
    requestUrl = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0/new_inscriptosporeventos(" + assistId + ")"

    requestHeaders = {
        'Authorization': 'Bearer ' + token,
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=utf-8',
        'Prefer': 'return=representation'
    }

    data =  {
        'new_asistencia' : True,
    }

    resHit = requests.patch(requestUrl, headers=requestHeaders, data=json.dumps(data))
    resHit = resHit.json()

    return resHit


def insertHit(token,campaignId,userId,userEmail):
    requestUrl = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0/new_inscriptosporeventos"

    requestHeaders = {
        'Authorization': 'Bearer ' + token,
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=utf-8',
        'Prefer': 'return=representation'
    }

    data =  {
        'new_inscriptoevento@odata.bind' : '/campaigns(' + campaignId + ')',
        'new_nombreyapellido@odata.bind' : '/contacts(' + userId + ')',
        'aw_email' : userEmail,
        'new_asistencia' : True
    }

    resHit = requests.post(requestUrl, headers=requestHeaders, data=json.dumps(data))
    resHit = resHit.json()

    return resHit


def checkHit(token,campaignId,userId,userEmail):
    apiUrl = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0"
    queryUrl = "/new_inscriptosporeventos?$filter=_new_inscriptoevento_value eq " + campaignId + " and _new_nombreyapellido_value eq " + userId
    requestUrl = apiUrl + queryUrl

    requestHeaders = {
        'Authorization': 'Bearer ' + token,
        'OData-MaxVersion': '4.0',
        'OData-Version': '4.0',
        'Accept': 'application/json',
        'Content-Type': 'application/json; charset=utf-8',
        'Prefer': 'return=representation'
    }

    resHit = requests.get(requestUrl, headers=requestHeaders)
    resHit = resHit.json()

    if resHit['value'] == []:
        insertHit(token,campaignId,userId,userEmail)
        return True
    else:
        r = resHit['value'][0]
        if r['new_asistencia'] == True:
            if reunionGrupal:
                insertHit(token,campaignId,userId,userEmail)
                return True
            else:
                return False
        elif r['new_asistencia'] == False:
            activateHit(token, r['new_inscriptosporeventoid'])
            return True
        

def queryContactsWithAccountFaseOne(accountId,campaignId,contactList):
    contactName = contactList["firstname"]
    contactLastname = contactList["lastname"]
    contactEmail = contactList["email"]
    contactAccount = contactList["account"]
    contactJob = contactList["jobtitle"]
    
    # Es la misma query para todas las fases, las diferencias estan en la query para matchear account
    queryUrlContacts = "/contacts?$select=fullname,emailaddress1,_parentcustomerid_value,statecode,contactid&$filter=_parentcustomerid_value eq " + accountId + " and contains(fullname,'" + contactLastname + "') and contains(fullname,'" + contactName + "')"
    crmres = requests.get(apiUrl+queryUrlContacts, headers=crmrequestheaders)
    
    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']

        # aca agrego lo de nombreTD y apellidoTD porque sino le agrega el hit a cualquier contacto de esa cuenta que tenga ese nombre y apellido (son muchos)
        if c == [] or  contactName == '(TD)NOMBRE' or contactLastname == '(TD)APELLIDO':
            # una misma linea creo el contacto y guardo su id en una variable            
            contactoCreadoId = createContact(tokenOauth,contactName,contactLastname,contactEmail,accountId,contactJob)
            #contactoCreadoId = ''

            if contactoCreadoId == 'Error 412':
                print('Ya existe un contacto con este correo electrónico ' + contactEmail)
            else:
                insertHit(tokenOauth,campaignId,contactoCreadoId,contactEmail)
                print('Fase 1: Se creo un contacto: ' + contactEmail)

                #print('Creo el contacto..FASE 1 ' + contactEmail)
        else:
            # Primero se fija si ya esta la asistencia cargada
            if checkHit(tokenOauth,campaignId,(c[0])['contactid'],(c[0])['emailaddress1']):
                # Si no esta cargada o tiene el asistio en No, ejecuta insertHit
                print("Fase 1: Se cargo el hit a un contacto ya existente " + (c[0])['emailaddress1'])
            else:
                print('Fase 1: Este contacto ya tiene cargada una asistencia en esta campaña: ' + contactEmail)
            
            #print('agrego el hit...FASE 1 '+ contactEmail)

    except KeyError:
        #handle any missing key errors
        print(contactList, 'Este contacto devuelve error')
        print('Could not parse CRM results')


def queryContactsWithAccountFaseTwo(accountId,campaignId,contactList):
    contactName = contactList["firstname"]
    contactLastname = contactList["lastname"]
    contactEmail = contactList["email"]
    contactAccount = contactList["account"]
    contactJob = contactList["jobtitle"]

    # Es la misma query para todas las fases, las diferencias estan en la query para matchear account
    queryUrlContacts = "/contacts?$select=fullname,emailaddress1,_parentcustomerid_value,statecode,contactid&$filter=_parentcustomerid_value eq " + accountId + " and contains(fullname,'" + contactLastname + "') and contains(fullname,'" + contactName + "')"
    crmres = requests.get(apiUrl+queryUrlContacts, headers=crmrequestheaders)

    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']
        if c == [] or  contactName == '(TD)NOMBRE' or contactLastname == '(TD)APELLIDO':
            # una misma linea creo el contacto y guardo su id en una variable
            contactoCreadoId = createContact(tokenOauth,contactName,contactLastname,contactEmail,accountId,contactJob)
            #contactoCreadoId = ''

            if contactoCreadoId == 'Error 412':
                print('Ya existe un contacto con este correo electrónico ' + contactEmail)
            else:
                insertHit(tokenOauth,campaignId,contactoCreadoId,contactEmail)
                print('Fase 2: Se creo un contacto: ' + contactEmail)

                #print('Creo el contacto..FASE 2 ' + contactEmail)
        else:
            if checkHit(tokenOauth,campaignId,(c[0])['contactid'],(c[0])['emailaddress1']):
                # Si no esta cargada o tiene el asistio en No, ejecuta insertHit
                print("Fase 2: Se cargo el hit a un contacto ya existente " + (c[0])['emailaddress1'])
            else:
                print('Fase 2: Este contacto ya tiene cargada una asistencia en esta campaña: ' + contactEmail)
            
            #print('agrego el hit...FASE 2 '+ contactEmail)

    except KeyError:
        #handle any missing key errors
        print(contactList, 'Este contacto devuelve error')
        print('Could not parse CRM results')



def queryContactsWithAccountFaseThree(accountId,campaignId,contactList):
    contactName = contactList["firstname"]
    contactLastname = contactList["lastname"]
    contactEmail = contactList["email"]
    contactAccount = contactList["account"]
    contactJob = contactList["jobtitle"]

    # Es la misma query para todas las fases, las diferencias estan en la query para matchear account
    queryUrlContacts = "/contacts?$select=fullname,emailaddress1,_parentcustomerid_value,statecode,contactid&$filter=_parentcustomerid_value eq " + accountId + " and contains(fullname,'" + contactLastname + "') and contains(fullname,'" + contactName + "')"
    crmres = requests.get(apiUrl+queryUrlContacts, headers=crmrequestheaders)

    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']
        if c == [] or  contactName == '(TD)NOMBRE' or contactLastname == '(TD)APELLIDO':
            # una misma linea creo el contacto y guardo su id en una variable
            contactoCreadoId = createContact(tokenOauth,contactName,contactLastname,contactEmail,accountId,contactJob)
            #contactoCreadoId = ''

            if contactoCreadoId == 'Error 412':
                print('Ya existe un contacto con este correo electrónico ' + contactEmail)
            else:
                insertHit(tokenOauth,campaignId,contactoCreadoId,contactEmail)
                print('Fase 3: Se creo un contacto: ' + contactEmail)

                #print('Creo el contacto..FASE 3 ' + contactEmail)
        else:
            # Primero se fija si ya esta la asistencia cargada
            if checkHit(tokenOauth,campaignId,(c[0])['contactid'],(c[0])['emailaddress1']):
                # Si no esta cargada o tiene el asistio en No, ejecuta insertHit
                print("Fase 3: Se cargo el hit a un contacto ya existente " + (c[0])['emailaddress1'])
            else:
                print('Fase 3: Este contacto ya tiene cargada una asistencia en esta campaña: ' + contactEmail)
            
            #print('agrego el hit...FASE 3 '+ contactEmail)

    except KeyError:
        #handle any missing key errors
        print(contactList, 'Este contacto devuelve error')
        print('Could not parse CRM results')



def save_s3_file(file_name, folder):  
    try:
        s3 = boto3.client('s3')
        s3.upload_file(f'/tmp/{file_name}', 'xxxxxbucket', f'{folder}/{file_name}')
        return True
    except ClientError as e:
        print(f"Error al subir el archivo a S3: {e}")
        return False



def get_s3_object(file_name, folder):
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket="xxxxxbucket", Key=f"{folder}/{file_name}")
        object_content = response['Body'].read().decode('utf-8')
        return object_content
    except ClientError as e:
        print(f"Error al subir el archivo a S3: {e}")
        return False
    
    
    
def clear_s3_file(file_name, folder):
    try:
        s3 = boto3.client('s3')
        s3.delete_object(Bucket='xxxxxbucket', Key=f'{folder}/{file_name}')
        return True
    except ClientError as e:
        print(f"Error al limpiar el archivo en S3: {e}")
        return False
 
 
    
def activateContact(contactIdUser):
    queryUrlActivate = "https://xxxxxcloud.crm2.dynamics.com/api/data/v9.0/contacts(" + contactIdUser + ")"

    # Cargar campos actualizados
    data =  {
        'statecode' : 0,
        'statuscode' : 1
    }

    #Aca se habilita el contacto 
    requests.patch(queryUrlActivate, headers=crmrequestheaders, data=json.dumps(data))

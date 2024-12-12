import requests
import csv
from modulo import *
import sys

# Traigo los asistentes desde el S3
asistentes = get_s3_object("testborrar.csv", "hits").splitlines()

# Creo las listas donde van a desembocar las coincidencias y las no coincidencias
lista_response_coincidencias = []
# A las no coincidencias las pongo en diferentes listas con la fase en el titulo porque no puedo poner todo en una sola (estas son las listas a las que les hago el for, entonces no puedo hacerles el append en el mismo paso)
lista_response_no_coincidencias_fase_0 = []
lista_response_no_coincidencias_fase_1 = []
lista_response_no_coincidencias_fase_2 = []
lista_response_no_coincidencias_fase_3 = []

# FASE 0 (match por email)
for i, asistente in enumerate(asistentes):
    # Con esto saco los headers
    if i == 0:
        continue
    
    # Elimino caracteres especiales y espacios extra en los datos del asistente
    replacements = {"; ": ";", " ;": ";", "?": "", "´": "%60", "'": "%60", "&": "%26"}

    for key, value in replacements.items():
        asistente = asistente.replace(key, value)
        
    print(asistente)

    # Separo los datos
    lista = asistente.split(";")
    name = lista[0]
    lastname = lista[1]
    email = lista[2]
    account = lista[3]
    job = lista[4].replace("\n","")

    # Agrego placeholders
    if name == '' or 'N/A' in name:
        name = '(TD)NOMBRE'
   
    if lastname == '' or 'N/A' in lastname:
        lastname = '(TD)APELLIDO'
      
    if account == '':
        account = '(TD)CUENTA'
    
    if job == '':
        job = '(TD)PUESTO'

    # Consigo el dominio
    x = email.split("@")
    domain = x[1].split(".")
    domain = '@' + domain[0] + '.'

    # Armo la query
    queryUrlContacts = "/contacts?$select=fullname,emailaddress1,contactid,statecode&$filter=emailaddress1 eq '" + email + "'"

    # Hago el request al CRM
    crmres = requests.get(apiUrl+queryUrlContacts, headers=crmrequestheaders)

    try:
        # Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']
        
        # Veo si hay match y en funcion de eso agrego el objeto a la lista correspondiente
        if email == 'meetings@amchamar.com.ar':
            print('Encontramos meetings@amchamar.com.ar') 
        elif c == []:
            response_no_coincidencia_fase_0 = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 0"
            }
            
            lista_response_no_coincidencias_fase_0.append(response_no_coincidencia_fase_0)
        else:
            # La funcion checkHit devuelve True si la asistencia no estaba en la campaña y se le cargo el hit
            if checkHit(tokenOauth,campaignId,(c[0])['contactid'],(c[0])['emailaddress1']):             
                response_coincidencia= {
                    "firstname" : name,
                    "lastname" : lastname,
                    "email" : email,
                    "account" : account,
                    "jobtitle" : job,
                    "fase" : "fase 0",
                    "CRM match" : (c[0])['fullname'] + " " + (c[0])['emailaddress1']
                }
                
                print('El contacto con el email ' + email + ' ya estaba en CRM y se le agrego una asistencia')
                
                lista_response_coincidencias.append(response_coincidencia)

                # Activo el contacto inactivo que hizo match directo con su email
                if (c[0])['statecode'] == 1:
                    activateContact((c[0])['contactid'])

            else:
                print('Este contacto ya tiene cargada una asistencia en esta campaña', email)
    except KeyError:
        #handle any missing key errors
        print('Could not parse CRM results')


# FASE 1 (match por dominio)
for response_no_coincidencia_fase_0 in lista_response_no_coincidencias_fase_0:
    # Separo los datos
    name = response_no_coincidencia_fase_0["firstname"]
    lastname = response_no_coincidencia_fase_0["lastname"]
    email = response_no_coincidencia_fase_0["email"]
    account = response_no_coincidencia_fase_0["account"]
    job = response_no_coincidencia_fase_0["jobtitle"]
    
    print(email + " emtro a la fase 1")

    # Consigo el dominio
    x = email.split("@")
    domain = x[1].split(".")
    # Le agregamos el @ y el . porque sino hacia match con mails equivocados
    domain = '@' + domain[0] + '.'
    
    # Le digo que no tome en consideracion a las cuentas genericas porque estas tienen distintos tipos de dominio
    # Independientes = dd49609c-c6ba-e811-a922-005056880cbf y Periodistas / Prensa / KOL = 65788513-f0bd-e911-a811-000d3ac02bb3
    notEq = "and _parentcustomerid_value ne dd49609c-c6ba-e811-a922-005056880cbf and _parentcustomerid_value ne 65788513-f0bd-e911-a811-000d3ac02bb3"      

    # Esto es para matchear con la cuenta y sacar el dominio, no el contacto
    queryUrlContacts = "/contacts?$select=fullname,emailaddress1,_parentcustomerid_value,statecode,contactid&$filter=contains(emailaddress1,'" + domain + "') and statecode eq 0 " + notEq
    
    crmres = requests.get(apiUrl+queryUrlContacts, headers=crmrequestheaders)
    
    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']
        # Los dominios genericos son considerados igual que los que no hacen match
        if c == [] or domain == '@gmail.' or domain == '@hotmail.' or domain == '@yahoo.' or domain == '@live.' or domain == '@outlook.' or domain == '@speedy.' or domain == '@fibertel.':
            response_no_coincidencia_fase_1 = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 1"
            }
            
            lista_response_no_coincidencias_fase_1.append(response_no_coincidencia_fase_1)        
        else:
            accountId = (c[0])['_parentcustomerid_value']
            
            # Aca adentro estan createContact, insertHit y checkHit. Le paso la cuenta como parametro
            # Esta funcion busca en la cuenta un contacto que haga match por nombre y apellido. Si hay match, agrega el hit al contacto. Si no hay match, crea el contacto y agrega el hit
            queryContactsWithAccountFaseOne(accountId,campaignId,response_no_coincidencia_fase_0)
            
            response_coincidencia = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 1",
            }
            
            lista_response_coincidencias.append(response_coincidencia)
            
    except KeyError:
        #handle any missing key errors
        print(lista, 'este tira error')
        print('Could not parse CRM results')


# FASE 2 (match por account eq)
for response_no_coincidencia_fase_1 in lista_response_no_coincidencias_fase_1:
    # Separo los datos
    name = response_no_coincidencia_fase_1["firstname"]
    lastname = response_no_coincidencia_fase_1["lastname"]
    email = response_no_coincidencia_fase_1["email"]
    account = response_no_coincidencia_fase_1["account"]
    job = response_no_coincidencia_fase_1["jobtitle"]
    
    print(email + " emtro a la fase 2")


    # Elimino caracteres especiales (creo que esto lo puedo borrar porque ya lo hago para todos en la linea 26)
    account = account.replace("´", "%60")
    account = account.replace("'", "%60")
    account = account.replace("&", "%26")
    
    # Obtengo la cuenta con la que hace match
    queryUrlAccounts = "/accounts?$select=name,new_nombrefacturaciondecuotasyservicios,new_condicion&$filter=name eq '" + account + "' or new_nombrefacturaciondecuotasyservicios eq '" + account + "'"
    crmres_accounts = requests.get(apiUrl+queryUrlAccounts, headers=crmrequestheaders)

    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres_accounts.json()
        c = crmresults['value']

        if c == []:
            response_no_coincidencia_fase_2 = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 2"
            }
            
            lista_response_no_coincidencias_fase_2.append(response_no_coincidencia_fase_2)                
        else:
            accountId = (c[0])['accountid']
            
            # Aca adentro estan createContact, insertHit y checkHit. Le paso la cuenta como parametro
            # Esta funcion busca en la cuenta un contacto que haga match por nombre y apellido. Si hay match, agrega el hit al contacto. Si no hay match, crea el contacto y agrega el hit
            queryContactsWithAccountFaseTwo(accountId,campaignId,response_no_coincidencia_fase_1)  
            
            response_coincidencia = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 2"
            }
            
            lista_response_coincidencias.append(response_coincidencia)
            
    except KeyError:
        #handle any missing key errors
        print('Could not parse CRM results')


# FASE 3
for response_no_coincidencia_fase_2 in lista_response_no_coincidencias_fase_2:
    name = response_no_coincidencia_fase_2["firstname"]
    lastname = response_no_coincidencia_fase_2["lastname"]
    email = response_no_coincidencia_fase_2["email"]
    account = response_no_coincidencia_fase_2["account"]
    job = response_no_coincidencia_fase_2["jobtitle"]
    
    print(email + " emtro a la fase 3")
    
    # Misma query que la primera de segunda fase pero con contains
    queryUrlAccounts = "/accounts?$select=name,new_nombrefacturaciondecuotasyservicios,new_condicion&$filter=contains(name,'" + account + "') or contains(new_nombrefacturaciondecuotasyservicios,'" + account + "')" 

    crmres = requests.get(apiUrl+queryUrlAccounts, headers=crmrequestheaders)

    try:
        #Obtengo el resultado de la llamada API
        crmresults = crmres.json()
        c = crmresults['value']
        # Traemos solo los que tuvieron una sola coincidencia. Si no hubo coincidencia o hubo varias, va en las no coincidencias
        if len(c) != 1:  
            if len(c) >= 2:
                response_no_coincidencia_fase_3 = {
                    "firstname" : name,
                    "lastname" : lastname,
                    "email" : email,
                    "account" : account,
                    "jobtitle" : job,
                    "fase" : "fase 3 multiple account matches"
                }
                
                lista_response_no_coincidencias_fase_3.append(response_no_coincidencia_fase_3)  
            else:
                response_no_coincidencia_fase_3 = {
                    "firstname" : name,
                    "lastname" : lastname,
                    "email" : email,
                    "account" : account,
                    "jobtitle" : job,
                    "fase" : "fase 3 no match"
                }
                
                lista_response_no_coincidencias_fase_3.append(response_no_coincidencia_fase_3)                  
        else:
            accountId = (c[0])['accountid']
            
            # Aca adentro estan createContact, insertHit y checkHit. Le paso la cuenta como parametro
            # Esta funcion busca en la cuenta un contacto que haga match por nombre y apellido. Si hay match, agrega el hit al contacto. Si no hay match, crea el contacto y agrega el hit
            queryContactsWithAccountFaseThree(accountId,campaignId,response_no_coincidencia_fase_2)
            
            response_coincidencia = {
                "firstname" : name,
                "lastname" : lastname,
                "email" : email,
                "account" : account,
                "jobtitle" : job,
                "fase" : "fase 3"
            }
            
            lista_response_coincidencias.append(response_coincidencia)

    except KeyError:
        #handle any missing key errors
        print('Could not parse CRM results')


# Guardo los listados de coincidencias y no coincidencias en el S3
json_coincidencias = {"results": lista_response_coincidencias}
json_no_coincidencias = {"results": lista_response_no_coincidencias_fase_3}

"""
with open(f"/tmp/coincidencias_{campaignName}.json", "w", encoding = 'utf-8') as match:
    json.dump(json_coincidencias, match)

if save_s3_file(f"coincidencias_{campaignName}.json", "hits"):
    print("El archivo fue guardado correctamente en S3")
else:
    print("Hubo un problema al guardar el archivo en S3") 
    
with open(f"/tmp/no_coincidencias_{campaignName}.json", "w", encoding = 'utf-8') as no_match:
    json.dump(json_no_coincidencias, no_match)
    
if save_s3_file(f"no_coincidencias_{campaignName}.json", "hits"):
    print("El archivo fue guardado correctamente en S3")
else:
    print("Hubo un problema al guardar el archivo en S3")
"""
if json_no_coincidencias['results'] != []:
    for i in json_no_coincidencias['results']:
        with open('backend-sistema-hits/noMatches.csv', 'a', encoding= 'utf-8') as f:
            f.write(str(i) + '\n')
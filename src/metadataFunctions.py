import json
import pymongo
from datetime import datetime, timezone
import pytz
from connection import getSchemaDBConnection, SCHEMA_DATABASE_NAME
import logging
from collections import defaultdict



def atualizaMetadadosCollection(docMetadados:dict):
    """
    Atualização de metadados da colection.
    Argumentos: 
    servidor - variavel do tipo MongoClient com a conexão com o banco Mongo onde serão mantidos os metadados.
    docMetadados - documento json com os metadados a serem atualizados

    """
    
    dtnow = datetime.now()
    fuso = pytz.timezone("America/Sao_Paulo")
    dtNow = fuso.localize(dtnow)


    if docMetadados["db"] == SCHEMA_DATABASE_NAME and docMetadados["collection"]=="colecaoMongo":
        return {'codigo': 100, 'message':"gravação do proprio schema do monitor de metadados não deve ser realizada"}

    #coleção e BD de armazenamento
    schemaDB = getSchemaDBConnection()

    #coleção usada para armazenamento
    schemaCollection  = schemaDB[SCHEMA_DATABASE_NAME]["colecaoMongo"] 
    

    docCompletoInclusao = {
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"],
        "estrutura" : [],
        "indConformidade" : None,
        "dtInclusao": dtNow
    }
    docEstruturaInclusao = {
        "versao": docMetadados["versao"],
        "quantidadeRegistros" : 1 ,
        "campos": [],
        "dtInclusao": dtNow
    }


    #definindo o array de campos
    arrayCamposInclusao = []
    for nome,tipo in docMetadados["estrutura"].items():
        docCampo= {
        "nomeFisico": nome,
        "tipo" : tipo
        }
        arrayCamposInclusao.append(docCampo)

    docEstruturaInclusao["campos"] = arrayCamposInclusao

    docCompletoInclusao["estrutura"] = [docEstruturaInclusao]

 
    # logging.debug("atualizaMetadadosCollection - Documento Completo:%s",json.dumps(docCompletoInclusao))

    #Processo de atualização dos metadados no repositório
    chaveDoc = { 
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"]}

    docDB = schemaCollection.find_one(chaveDoc)
    
    if not docDB:
        # inserindo novo documento na collection
        infoCmdDB = schemaCollection.insert_one(docCompletoInclusao)
        if infoCmdDB.acknowledged:
            # print(infoInsert)
            return {'codigo': 000, 'message':"Documento Inserido"}

    #Atualizando Estrutura do documento existente
    indEstrutura = False
    for estrutRet in docDB["estrutura"]:
        if estrutRet["versao"] == docEstruturaInclusao["versao"]:
            indEstrutura=True

    if not indEstrutura:
        chaveDoc["estrutura.versao"]={'$ne':docEstruturaInclusao["versao"]}                               #evita adicionar estrutura se já tiver outra no BD
        # print('\nInserindo estrutura para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"]) 
        infoCmdDB = schemaCollection.update_one(chaveDoc,{'$addToSet': {'estrutura':docEstruturaInclusao}})  #risco de updates simultaneos gerar inconsistencia
        #se atualizou(incluiu a estrutura), retorna ok, senão, continua para atualizar a estrutura
        if infoCmdDB.modified_count > 0 :
            # print('insert estrutura:', infoInsert.raw_result)    
            return {'codigo': 000, 'message':"Documento já existe. Versão da estrutura adicionada"}
    
    # print('\nAtualizando campos para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"])
    docAtualizacao = {"$inc": {"estrutura.$[vrs].quantidadeRegistros":1},
                    "$addToSet": {"estrutura.$[vrs].campos":{ '$each': arrayCamposInclusao}}
                    }
    filtroAtualizacao= [{"vrs.versao": docEstruturaInclusao["versao"]} ]
    infoCmdDB = schemaCollection.update_one(chaveDoc,docAtualizacao, array_filters=filtroAtualizacao)
    # print('add campos:', 'match:', infoInsert.matched_count,'atualizados',infoInsert.modified_count)    
    return {'codigo': 000, 'message':"Estrutura Atualizada"}

def atualizaMetadadosCollection2(docMetadados:dict):
    """
    Atualização de metadados da colection.
    Argumentos: 
    servidor - variavel do tipo MongoClient com a conexão com o banco Mongo onde serão mantidos os metadados.
    docMetadados - documento json com os metadados a serem atualizados

    """
    
    dtnow = datetime.now()
    fuso = pytz.timezone("America/Sao_Paulo")
    dtNow = fuso.localize(dtnow)
 


    if docMetadados["db"] == SCHEMA_DATABASE_NAME and docMetadados["collection"]=="colecaoMongo":
        return {'codigo': 100, 'message':"gravação do proprio schema do monitor de metadados não deve ser realizada"}

    #coleção e BD de armazenamento
    schemaDB = getSchemaDBConnection()

    #coleção usada para armazenamento
    schemaCollection  = schemaDB[SCHEMA_DATABASE_NAME]["colecaoMongo2"] 
    estruturaCollection = schemaDB[SCHEMA_DATABASE_NAME]["estruturaColecao2"] 
    

    docCompletoInclusao = {
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"],
        "estrutura_id" : [],
        "indConformidade" : None,
        "dtInclusao": dtNow
    }
    docEstruturaInclusao = {
        "collection_id" : 0 ,
        "versao": docMetadados["versao"],
        "qtRegistros" : 1 ,
        "campos": {},
        "dtInclusao": dtNow
    }


    #definindo o array de campos
    # arrayCamposInclusao = []
    docCampo = {}
    for nome,tipo in docMetadados["estrutura"].items():
        docCampo[nome]= { 
            "nomeFisico": nome,
            "tipo" : [tipo]
        }
        # arrayCamposInclusao.append(docCampo)

    # docEstruturaInclusao["campos"] = arrayCamposInclusao
    docEstruturaInclusao["campos"] = docCampo

    # print("1-docEstruturaInclusao",docEstruturaInclusao)
 
    # logging.debug("atualizaMetadadosCollection - Documento Completo:%s",json.dumps(docCompletoInclusao))

    #Processo de atualização dos metadados no repositório
    chaveCollection = { 
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"]}
    chaveEstrutura = {
        "collection_id" : 0, #atualizar a do recuperado ou inserido
        "versao": docMetadados["versao"]
    }

    docCollection = schemaCollection.find_one(chaveCollection)
    idCollection = ""
    if not docCollection:
        # inserindo novo documento na collection
        infoCmdDB = schemaCollection.insert_one(docCompletoInclusao)
        idCollection = infoCmdDB.inserted_id
    else:
        idCollection = docCollection["_id"]


        # if infoCmdDB.acknowledged:
        #     # print(infoInsert)
        #     return {'codigo': 000, 'message':"Documento Inserido"}

    chaveEstrutura["collection_id"] = idCollection
    docEstruturaInclusao["collection_id"] = idCollection

    # print("chaveEstrutura=",chaveEstrutura) 
    # print("docEstruturaInclusao=",docEstruturaInclusao)    
    # print("docCampo=",docCampo)

    #Atualizando Estrutura do documento existente
    docEstrutura = estruturaCollection.find_one(chaveEstrutura)
    idEstrutura = ''
    if not docEstrutura:
        infoCmdDB = estruturaCollection.insert_one(docEstruturaInclusao)
        idEstrutura = infoCmdDB.inserted_id
        infoCmdDB = schemaCollection.update_one(chaveCollection,{"$addToSet":{"estrutura_id":idEstrutura}})
        return {'codigo': 000, 'message':"Estrutura Inserida"}
    else:
        # chaveDoc["estrutura.versao"]={'$ne':docEstruturaInclusao["versao"]}                               #evita adicionar estrutura se já tiver outra no BD
        # print('\nInserindo estrutura para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"]) 
        docNomes={}
        docTipo={}
        docEstruturaUpdate={}
        for nome,tipo in docMetadados["estrutura"].items():
            docNomes["campos."+nome+".nomeFisico"]=nome
            docTipo["campos."+nome+".tipo"]=tipo
        docEstruturaUpdate.update({"$addToSet":docTipo})   #está atualizando apenas um (sobrepoe a chave do documento anterior)
        docEstruturaUpdate.update({"$set": docNomes})
        docEstruturaUpdate.update({"$inc":{"qtRegistros":1}})
        
        # print("###> docAddSet=",docEstruturaUpdate)
        # return {'codigo': 000, 'message':"Estrutura Simulada"}

        infoCmdDB = estruturaCollection.update_one(chaveEstrutura,docEstruturaUpdate)  #risco de updates simultaneos gerar inconsistencia
        #se atualizou(incluiu a estrutura), retorna ok, senão, continua para atualizar a estrutura
        if infoCmdDB.modified_count == 0 :
            # print('insert estrutura:', infoInsert.raw_result)    
            return {'codigo': 000, 'message':"Documento já existe. Sem Atualizações"}
    
        # print('\nAtualizando campos para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"])
        # docAtualizacao = {"$inc": {"estrutura.$[vrs].quantidadeRegistros":1},
        #                 "$addToSet": {"estrutura.$[vrs].campos":{ '$each': arrayCamposInclusao}}
        #                 }
        # filtroAtualizacao= [{"vrs.versao": docEstruturaInclusao["versao"]} ]
        # infoCmdDB = schemaCollection.update_one(chaveDoc,docAtualizacao, array_filters=filtroAtualizacao)
        # print('add campos:', 'match:', infoInsert.matched_count,'atualizados',infoInsert.modified_count)    
        return {'codigo': 000, 'message':"Estrutura Atualizada"}

def atualizaMetadadosCollection3(docMetadados:dict):
    """
    Atualização de metadados da colection.
    Argumentos: 
    servidor - variavel do tipo MongoClient com a conexão com o banco Mongo onde serão mantidos os metadados.
    docMetadados - documento json com os metadados a serem atualizados

    """
    
    dtNowUTC = datetime.utcnow()

    if docMetadados["db"] == SCHEMA_DATABASE_NAME and docMetadados["collection"]=="colecaoMongo":
        return {'codigo': 100, 'message':"gravação do proprio schema do monitor de metadados não deve ser realizada"}

    #coleção e BD de armazenamento
    schemaDB = getSchemaDBConnection()

    #coleção usada para armazenamento
    schemaCollection  = schemaDB[SCHEMA_DATABASE_NAME]["colecaoMongoV3"] 
    # estruturaCollection = schemaDB[SCHEMA_DATABASE_NAME]["estruturaColecao2"] 
    

    docCompletoInclusao = {
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"],
        "estrutura" : {},
        "indConformidade" : None,
        "dtInclusao": dtNowUTC
    }

    docEstruturaInclusao={}

    # Definindo estrutura de armazenamento da versão do schema. A chave da estrutura será "versao-nnn"
    keyVersao = "versao-"+ (str(docMetadados["versao"]).replace('.','_'),'0')[docMetadados["versao"]==None] #concatena com 0 se não tiver versao (troca '.' da versão por '_')
    docEstruturaInclusao[keyVersao] = { 
            "versao": docMetadados["versao"],
            "qtRegistros" : 1 ,
            "qtCampos": 0,
            "campos": {},
            "dtInclusao": dtNowUTC
            }

    # definindo o array de campos
    docCampo = {}
 
    for nomeCampo,tipoCampo in docMetadados["estrutura"].items():
        keyCampo=str(nomeCampo).replace('.','/')
        docCampo[keyCampo]= { 
            "nomeFisicoCompleto": nomeCampo,
            "nomeFisico": str(nomeCampo).split('.')[-1] ,
            "dtInclusao": dtNowUTC,
            "tipo" : [tipoCampo]
        }
        docEstruturaInclusao[keyVersao]["qtCampos"] += 1
        # arrayCamposInclusao.append(docCampo)

    docEstruturaInclusao[keyVersao]["campos"] = docCampo
    docCompletoInclusao["estrutura"] = docEstruturaInclusao

    # print("1-docEstruturaInclusao",docEstruturaInclusao)
    # print("2-docCompletoInclusao",docCompletoInclusao)
 
    # logging.debug("atualizaMetadadosCollection - Documento Completo:%s",json.dumps(docCompletoInclusao))

    #Processo de atualização dos metadados no repositório
    chaveCollection = { 
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"]}
    # chaveEstrutura = {
    #     "collection_id" : 0, #atualizar a do recuperado ou inserido
    #     "versao": docMetadados["versao"]
    # }

    docCollection = {}
    docCollection = schemaCollection.find_one(chaveCollection)

    # idCollection = ""
    if not docCollection:
        # inserindo novo documento na collection
        infoCmdDB = schemaCollection.insert_one(docCompletoInclusao)
        if infoCmdDB.acknowledged:
            # print(infoInsert)
            return {'codigo': 000, 'message':"Documento Inserido"}

    # chaveEstrutura["collection_id"] = idCollection
    # docEstruturaInclusao["collection_id"] = idCollection

    # print("chaveEstrutura=",chaveEstrutura) 
    # print("docEstruturaInclusao=",docEstruturaInclusao)    
    # print("docCollection=",docCollection)

    # Agualização do documento existente
    # ##################################
 
    docEstruturaUpdate={}


    # Se versão da estrutura não existe na estrutura, inclui.
    if docCollection.get("estrutura").get(keyVersao) == None :
        docEstruturaUpdate.update({"$set":{"estrutura."+keyVersao:docEstruturaInclusao[keyVersao]}})
        docEstruturaUpdate.update({"$currentDate":{"dtUltimaAtualizacao":True}})
        # print("docEstruturaUpdate-Versao=",docEstruturaUpdate)
        infoCmdDB = schemaCollection.update_one(chaveCollection,docEstruturaUpdate)
        if infoCmdDB.modified_count == 0 :
            # print('insert estrutura:', infoInsert.raw_result)    
            return {'codigo': 000, 'message':"Inclusão de versao em Estrutura sem atualização"}
        else:
            return {'codigo': 000, 'message':"Versao de Estrutura incluída"}


    docNomes={}
    docTipo={}
    docEstruturaUpdate={}
    qtCamposAdd=0
    for nomeCampo,tipoCampo in docMetadados["estrutura"].items():
        # print("3-Avaliando nome:", "estrutura."+keyVersao+".campos."+nome+".nomeFisico")
        # print("4-Avaliando tipo:", "estrutura."+keyVersao+".campos."+nome+".tipo")
        
        keyCampo=str(nomeCampo).replace('.','/')

        indInserirNome = False
        indInserirTipo = False
        
        if docCollection.get("estrutura").get(keyVersao).get("campos").get(keyCampo) == None :
           indInserirNome = True
           indInserirTipo = True
        else:
            # print("5-getDocCollections nome",str(docCollection.get("estrutura").get(keyVersao).get("campos").get(nome).get("nomeFisico")))
            # print("6-getDocCollections tipo",str(docCollection.get("estrutura").get(keyVersao).get("campos").get(nome).get("tipo")))

            if docCollection.get("estrutura").get(keyVersao).get("campos").get(keyCampo).get("nomeFisico") == None :
                indInserirTipo=True
            if tipoCampo not in docCollection.get("estrutura").get(keyVersao).get("campos").get(keyCampo).get("tipo") :    
                indInserirTipo=True
        
                
        if indInserirNome :
            docNomes["estrutura."+keyVersao+".campos."+keyCampo+".nomeFisicoCompleto"]=nomeCampo
            docNomes["estrutura."+keyVersao+".campos."+keyCampo+".nomeFisico"]=str(nomeCampo).split('.')[-1]
            docNomes["estrutura."+keyVersao+".campos."+keyCampo+".dtInclusao"]=dtNowUTC
            qtCamposAdd += 1
           
        if indInserirTipo :  
            docTipo["estrutura."+keyVersao+".campos."+keyCampo+".tipo"]=tipoCampo
            
    
    # Gerando comandos de update
    if len(docTipo) > 0:        
        docEstruturaUpdate.update({"$addToSet":docTipo})   #está atualizando apenas um (sobrepoe a chave do documento anterior)
    if len(docNomes) > 0:    
        docEstruturaUpdate.update({"$set": docNomes})
    if qtCamposAdd > 0:
        docEstruturaUpdate.update({"$inc":{"estrutura."+keyVersao+".qtCampos":1}})
    
    # print("###> docAddSet=",docEstruturaUpdate)
    # return {'codigo': 000, 'message':"Estrutura Simulada"}

    if len(docEstruturaUpdate) > 0:
        docEstruturaUpdate.update({"$currentDate":{"dtUltimaAtualizacao":True, "estrutura."+keyVersao+".dtUltimaAtualizacao":True} })

        infoCmdDB = schemaCollection.update_one(chaveCollection,docEstruturaUpdate)  #risco de updates simultaneos gerar inconsistencia
        #se atualizou(incluiu a estrutura), retorna ok, senão, continua para atualizar a estrutura
        if infoCmdDB.modified_count == 0 :
            # print('insert estrutura:', infoInsert.raw_result)    
            return {'codigo': 000, 'message':"Update em Estrutura sem atualização"}
        else:
            return {'codigo': 000, 'message':"Estrutura atualizada"}


    # print('\nAtualizando campos para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"])
    # docAtualizacao = {"$inc": {"estrutura.$[vrs].quantidadeRegistros":1},
    #                 "$addToSet": {"estrutura.$[vrs].campos":{ '$each': arrayCamposInclusao}}
    #                 }
    # filtroAtualizacao= [{"vrs.versao": docEstruturaInclusao["versao"]} ]
    # infoCmdDB = schemaCollection.update_one(chaveDoc,docAtualizacao, array_filters=filtroAtualizacao)
    # print('add campos:', 'match:', infoInsert.matched_count,'atualizados',infoInsert.modified_count)    
    return {'codigo': 000, 'message':"Estrutura sem Atualizações"}

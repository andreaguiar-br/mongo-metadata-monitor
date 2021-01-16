import json
import pymongo
from datetime import datetime, timezone
import pytz
from connection import getSchemaDBConnection



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


    if docMetadados["db"] == "mdbmmd" and docMetadados["collection"]=="colecaoMongo":
        return {'codigo': 100, 'message':"gravação do proprio schema do monitor de metadados não deve ser realizada"}

    #coleção e BD de armazenamento
    schemaDB = getSchemaDBConnection()

    #coleção usada para armazenamento
    schemaCollection  = schemaDB["mdbmmd"]["colecaoMongo"] 
    

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

    # print("\nmetadados:",docCompletoInclusao)

    #Processo de atualização dos metadados no repositório
    chaveDoc = { 
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"]}

    docDB = schemaCollection.find_one(chaveDoc)
    # print(chaveDoc)
    # print("\nachei?\n:",docDB)
    if not docDB:
        # print(True)
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
            return {'codigo': 000, 'message':"Estrutura Inserida"}
    
    # print('\nAtualizando campos para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"])
    docAtualizacao = {"$inc": {"estrutura.$[vrs].quantidadeRegistros":1},
                    "$addToSet": {"estrutura.$[vrs].campos":{ '$each': arrayCamposInclusao}}
                    }
    filtroAtualizacao= [{"vrs.versao": docEstruturaInclusao["versao"]} ]
    infoCmdDB = schemaCollection.update_one(chaveDoc,docAtualizacao, array_filters=filtroAtualizacao)
    # print('add campos:', 'match:', infoInsert.matched_count,'atualizados',infoInsert.modified_count)    
    return {'codigo': 000, 'message':"Estrutura Atualizada"}

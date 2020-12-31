import json
import pymongo
from datetime import datetime, timezone
import pytz

typePy2mongo = {
    "bool" : "bool",
    "int"  : "int32",
    "long" : "long",
    "Int64" : "long",
    "float" : "double",
    "str"   : "string",
    "dict"  : "object",
    "list"  : "array",
    "timestamp": "timestamp",
    "date"  : "date",
    "datetime": "datetime",
    "ObjectId": "objectId",
    "Binary" : "binData",
    "Decimal128": "decimal",
    "NoneType" : "null",
    "DBRef" : "dbref",
    "Byte" : "binData"

}
def getMetadados( docChangeStream ):
    '''
    Gera documento JSON com metadados extraídos do documento gerado pelo Change Stream do MongoDB.

    parametros:
    docChamStream - Documento gerado pelo evento de ChangeStream e que terá seus metadados extraídos

    retorna:
    documento com os metadados em padrão pre-definido.
    '''
    doctipo = getTypeDoc(docChangeStream["fullDocument"]) 
    # pprint.pprint(doctipo)
    # print("### Gravando Metadados ###")
    # identificando a versão da estrutura (schema)
    
    if "schema_version" in docChangeStream["fullDocument"]:
        versaoEstrutura=docChangeStream["fullDocument"]["schema_version"]
    else:
        versaoEstrutura=None

    return {"host":'localhost',
        "db": docChangeStream["ns"]["db"],
        "collection": docChangeStream["ns"]["coll"],
        "estrutura": doctipo,
        "versao": versaoEstrutura
        }
  

def getTypeDoc( docParm ):
    docTipo = {}
    for campo, valor in docParm.items() :
        if type(valor).__name__ == 'dict':
            docTipo[campo] =  typePy2mongo[type(valor).__name__ ]
            subDoc = getTypeDoc(valor)
            for campoSubDoc, valorSubDoc in subDoc.items():
                docTipo[campo+'.'+campoSubDoc]= valorSubDoc
        elif type(valor).__name__ == 'list':
            docArrayTipo = getTypeArrayDoc(valor)
            for campoDocArray, valorDocArray in docArrayTipo.items():
                docTipo[campo+''+campoDocArray]= valorDocArray
        else:
            docTipo[campo] =  typePy2mongo[type(valor).__name__ ]
            pass 
    return docTipo

def getTypeArrayDoc( arrayParm ):
    arrayTipo = []
    docArray = {'[]': [] }
    for elemento in arrayParm :
        if type(elemento).__name__ == 'dict':
            subDoc = getTypeDoc(elemento)
            # arrayTipo.append(subDoc)
            for campoSubDoc, valorSubDoc in subDoc.items():
                docArray['[].'+campoSubDoc]= valorSubDoc
        elif type(elemento).__name__ == 'list':
            subDocArray = getTypeArrayDoc(elemento)
            # arrayTipo.append(subArray)
            for campoSubDoc, valorSubDoc in subDocArray.items():
                if '[]'+campoSubDoc in docArray :
                    # se já existe, adiciona o tipo encontrato a lista de documentos
                    if type(docArray['[]'+campoSubDoc]).__name__=='list' :
                        try:
                            docArray['[]'+campoSubDoc].extend(valorSubDoc) 
                            #removendo duplicatas
                            docArray['[]'+campoSubDoc]=list(dict.fromkeys(docArray['[]'+campoSubDoc]))    
                        except ValueError:
                            pass
                    else:
                        #transforma valor existente em lista
                        # print('11111', "['"+docArray['[]'+campoSubDoc]+"','"+ valorSubDoc+"']")
                        docArray['[]'+campoSubDoc]=[ docArray['[]'+campoSubDoc] , valorSubDoc ] 
                else:
                    # primeiro registro do array, cria registro no documento
                    docArray['[]'+campoSubDoc]= valorSubDoc
        else:
            pass
        try:
            arrayTipo.index(typePy2mongo[type(elemento).__name__])
        except ValueError:
            arrayTipo.append(typePy2mongo[type(elemento).__name__])
    # print(arrayTipo)
    if len(arrayTipo) > 1 :
        docArray['[]']= arrayTipo
    else:
        docArray['[]']= arrayTipo[0]
    return docArray

# import json
# import pymongo
# import pymongo.results

def atualizaMetadadosCollection(servidor, docMetadados):
    """
    Atualização de metadados da colection.
    Argumentos: 
    servidor - variavel do tipo MongoClient com a conexão com o banco Mongo onde serão mantidos os metadados.
    docMetadados - documento json com os metadados a serem atualizados

    """
    
    dtnow = datetime.now()
    fuso = pytz.timezone("America/Sao_Paulo")
    dtNow = fuso.localize(dtnow)
    # returar a linha abaixo
    # servidor = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")

    if docMetadados["db"] == "mdbmmd" and docMetadados["collection"]=="colecaoMongo":
        return {'codigo': 100, 'message':"gravação do proprio schema do monitor de metadados não deve ser realizada"}
    #coleção e BD de armazenamento
    collectonStore = servidor["mdbmmd"]["colecaoMongo"]

    docCompletoInclusao = {
        "uriServidor": docMetadados["host"],
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

    # # identificando a versão da estrutura (schema)
    # if "schema_version" in docMetadados["estrutura"]:
    #     docEstruturaInclusao["versao"]=docMetadados["estrutura"]["schema_version"]
    # else:
    #     docEstruturaInclusao["versao"]=None


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
        "uriServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"]}

    docDB = collectonStore.find_one(chaveDoc)
    # print(chaveDoc)
    # print("\nachei?\n:",docDB)
    if not docDB:
        # print(True)
        infoCmdDB = collectonStore.insert_one(docCompletoInclusao)
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
        infoCmdDB = collectonStore.update_one(chaveDoc,{'$addToSet': {'estrutura':docEstruturaInclusao}})  #risco de updates simultaneos gerar inconsistencia
        #se atualizou(incluiu a estrutura), retorna ok, senão, continua para atualizar a estrutura
        if infoCmdDB.modified_count > 0 :
            # print('insert estrutura:', infoInsert.raw_result)    
            return {'codigo': 000, 'message':"Estrutura Inserida"}
    
    # print('\nAtualizando campos para ',docEstruturaInclusao["versao"], 'em', docDB["estrutura"])
    docAtualizacao = {"$inc": {"estrutura.$[vrs].quantidadeRegistros":1},
                    "$addToSet": {"estrutura.$[vrs].campos":{ '$each': arrayCamposInclusao}}
                    }
    filtroAtualizacao= [{"vrs.versao": docEstruturaInclusao["versao"]} ]
    infoCmdDB = collectonStore.update_one(chaveDoc,docAtualizacao, array_filters=filtroAtualizacao)
    # print('add campos:', 'match:', infoInsert.matched_count,'atualizados',infoInsert.modified_count)    
    return {'codigo': 000, 'message':"Estrutura Atualizada"}

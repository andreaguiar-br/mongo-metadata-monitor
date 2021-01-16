# import json
import pymongo
import pymongo.results


def atualizaMetadadosCollection(servidor: str, docMetadados: dict) -> dict:
    """
    Atualização de metadados da colection.
    Argumentos: 
    servidor - variavel do tipo MongoClient com a conexão com o banco Mongo onde serão mantidos os metadados.
    docMetadados - documento json com os metadados a serem atualizados

    """
    
    # returar a linha abaixo
    servidor = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")

    #coleção e BD de armazenamento
    collectonStore = servidor["mdbmmd"]["colecaoMongo"]

    docCompletoInclusao = {
        "nomeServidor": docMetadados["host"],
        "nomeDatabase" : docMetadados["db"],
        "nomeFisico": docMetadados["collection"],
        "estrutura" : [],
        "indConformidade" : False
    }
    docEstruturaInclusao = {
        "versao": "se tiver schema_version",
        "quantidadeRegistros" : 1 ,
        "campos": [],
    }

    # identificando a versão da estrutura (schema)
    if "schema_version" in docMetadados["estrutura"]:
        docEstruturaInclusao["versao"]=docMetadados["estrutura"]["schema_version"]
    else:
        docEstruturaInclusao["versao"]=None


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

# db.clienteOne.aggregate([{$match: {"_id":"X"}},{$project: { _id:0, newMatriz: {$concatArrays: ["$matriz", ["X"]]}}},{$project:{ "newMatriz":"$newMatriz", newMatrizSize:{ $size: "$newMatriz"}}}])
# retorna apenas o array campos 
# db.colecaoMongo.aggregate([{$match:{"_id":ObjectId('5fee466060ea66a9b058fcfb')}},{$unwind: "$estrutura"},{$match:{"estrutura.versao":2}},{$project:{"_id":0,"campos":"$estrutura.campos"}}]).pretty()
# db.colecaoMongo.aggregate([{$match:{"_id":ObjectId('5fee466060ea66a9b058fcfb')}},{$unwind: "$estrutura"},{$match:{"estrutura.versao":2}},{$project:{"_id":0,"campos":{$concatArrays: ["$estrutura.campos",[{"nomeFisico": "_id", "tipo": "bool","desc":"abcd"},{"nomeFisico":"sexo", "tipo":"string"}]]}}},{$unwind: "$campos"},{$group:{"_id":"$campos.nomeFisico", "campos":{"$last":"$campos"}}},{$project:{"_id":0}},{$group:{"_id":null,"campos": {$addToSet:"$campos"}}}]).pretty()
# db.colecaoMongo.aggregate([{$match:{"_id":ObjectId('5fee466060ea66a9b058fcfb')}},{$unwind: "$estrutura"},{$match:{"estrutura.versao":2}},{$project:{"_id":0,"campos":{$concatArrays: ["$estrutura.campos",[{"nomeFisico": "_id", "tipo": "bool","desc":"abcd"},{"nomeFisico":"sexo", "tipo":"string"}]]}}},{$unwind: "$campos"},{$group:{"_id":"$campos.nomeFisico", "campos":{"$mergeObjects":"$campos"}}},{$project:{"_id":0}},{$group:{"_id":null,"campos": {$addToSet:"$campos"}}}])
# pegar pipes anteriores e tentar agrupar o tipo em um array (ver se usa o $facet)
# db.colecaoMongo.aggregate([{$match:{"_id":ObjectId('5fee466060ea66a9b058fcfb')}},{$unwind: "$estrutura"},{$match:{"estrutura.versao":2}},{$project:{"_id":0,"campos":{$concatArrays: ["$estrutura.campos",[{"nomeFisico": "_id", "tipo": "bool","desc":"abcd"},{"nomeFisico":"sexo", "tipo":"string"}]]}}},{$unwind: "$campos"},{$project:{"_id":0, "campos.nomeFisico":1,"campos.tipo":1}},{$group:{"_id":"$campos.nomeFisico","tipo":{$addToSet:"$campos.tipo"}}}])
# Facet 1- db.colecaoMongo.aggregate([{$match:{"_id":ObjectId('5fee466060ea66a9b058fcfb')}},{$unwind: "$estrutura"},{$match:{"estrutura.versao":2}},{$project:{"_id":0,"campos":{$concatArrays: ["$estrutura.campos",[{"nomeFisico": "_id", "tipo": "bool","desc":"abcd"},{"nomeFisico":"sexo", "tipo":"string"}]]}}},{$unwind: "$campos"},{$project:{"_id":0, "campos.nomeFisico":1,"campos.tipo":1}},{$group:{"_id":"$campos.nomeFisico","tipo":{$addToSet:"$campos.tipo"}}},{$group:{"_id":null, "campos":{"$addToSet": {"nomeFisico":"$_id" , "tipo": "$tipo"}}}},{$project:{"_id":0}}]
# 
#     db.students.updateOne(
#    { grades: { $gte: 100 } },
#    { $set: { "grades.$[element]" : 100 } },
#    { arrayFilters: [ { "element": { $gte: 100 } } ] }
# 
# $inc: {"estrutura.$[vrs].quantidadeRegistros":1}
# $addToSet: {"estrutura.$[vrs].campos":docEstruturaInclusao["campos"]}
# arrayFilters: [{"vrs": {$elemMatch:{"versao":docEstruturaInclusao["versao"]}}} ]
    # docUpdate = {
    #     "$set": {
    #         "nomeServidor": docMetadados["host"],
    #         "nomeDatabase" : docMetadados["db"],
    #         "nomeFisico": docMetadados["collection"],
    #         "estrutura.$": docEstruturaInclusao
    #     }
    # }
    # collectonStore.update_one(upsert=true)
    return {'codigo': 999, 'info': "deu pau"}
'''
OK1) find do documento (host/bd/collection) - se não tiver, incluir o documento integral
OK1.1) existindo o documento - ver se tem estrutura com a versao no array - se não tiver, atualizar, inserindo array de docEstrutura
1.1.1) existindo a estrutura com a versão no array 
       - para cada documento no array campo, verifica se há no novo nome - 
            - inserir documento se não existir
            - atualizar tipo adicionando o novo tipo, se diferente

quando: "estrutura.$.versao" = x
update: "$inc": {"estrutura.$.quantidadeRegistros":1}
        "$addToSet: {estrutura.$.campos": "{$each:[ { nomeFisico:x, tipo:y},]"
'''
docEstrutura = {'_id': 'objectId',
#  'definicao': 'string',
#  'estrutura[]': 'object',
#  'estrutura[].campos[]': 'object',
#  'estrutura[].campos[].definicao': 'string',
#  'estrutura[].campos[].inconformidades[]': 'string',
#  'estrutura[].campos[].nomeFisico': 'string',
 'estrutura[].campos[].nomeLogico': 'string',
#  'estrutura[].campos[].tipo': 'string',
#  'estrutura[].quantidadeRegistros': 'double',
#  'estrutura[].versao': 'double',
 'indConformidade': 'bool',
 'nomeDatabase': 'string',
 'schema_version': 3 ,
 'nomeFisico': 'string',
 'nomeLogico': 'string',
 'nomeServidor': 'string'}

docMetadados = {
     "host": "localhost",
     "db": "kaggle",
     "collection": "teste",
     "estrutura": docEstrutura
 }

info = atualizaMetadadosCollection('conexao',docMetadados)
print('Resultado:' , info)
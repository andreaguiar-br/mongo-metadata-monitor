# mongoMetadataMonitor
Projeto para capturar documentos incluídos e alterados no mongoDB, extrair estrutura dos dados e seus tipos, e enviar para base de catalogação para proporcionar gestão (identificação de tipos e campos diferentes nas coleções, efetuar categorizações de dados e descrição de atributos)

Pretende-se fazer conexão com o [ChangeStream](https://docs.mongodb.com/manual/changeStreams/) do MongoDB, no nível de instalação, para pegar diferentes BDs.

# Execução principal
Programa [ChangeMonitor.py](src/ChangeMonitor.py) é o que monitora o stream de mudanças do Cluster MongoDB. Então, qualquer mudança em qualquer database o programa recebe um evento e o documento completo alterado.

Com o documento alterado, executa uma função para gerar um documento com o nome e datatype do documento recebido no evento, e envia para uma função de armazenamento do metadado em formato [específico](info/ExemploEstrutura.json)

# Ambiente local para testes
Utilize este [docker compose](/test/mongoDB/docker-compose.yml) do MongoDB. Basta executar `docker-compose up -d` na pasta [test/mongoDB](test/mongoDB)

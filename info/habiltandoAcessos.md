# Habilitação de Acessos para funcionamento do monitoramento
Para funcionamento do monitor de metadados, é necessário criar no MongoDB:

* Usuário "MDBIMMD" com acesso <read> se for monitorar um database ou <readAnyDatabase> se for monitorar os BDs de uma instalação MongoDB
* Usuário "MDBIMMD" com acesso <readWrite> para o database <mdbmmd-metadataMonitor> para gravação dos schemas

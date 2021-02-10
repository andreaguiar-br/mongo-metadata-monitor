# Habilitação de Acessos para funcionamento do monitoramento
Para funcionamento do monitor de metadados, é necessário criar no MongoDB:

## Usuário "MDBIMMD" com acesso <read> se for monitorar um database ou <readAnyDatabase> se for monitorar os BDs de uma instalação MongoDB
  Se for monitorar um database e já tiver criado o usuário no database **admin**
  ```
  use admin
  db.grantRolesToUser( "MDBIMMD01", [ {role:"read", db:"<database>" } ] ) 
  ```
  
  Se for monitorar a instalação MongoDB, crie o usuário já com as permissões
  ```
  use admin
  db.createUser({user:"MDBIMMD01", pwd:"meta$monitor", roles:[ {role:"readAnyDatabase", db:"admin" }]})
  ```
  ou se já tiver criado o usuário no database **admin**, apenas adicione a permissão
  ```
  use admin
  db.grantRolesToUser( "MDBIMMD01", [ {role:" readAnyDatabase", db:"admin" } ] )
  ```
 
## Usuário "MDBIMMD" com acesso <readWrite> para o database <mdbmmd-metadataMonitor> para gravação dos schemas
  Crie o usuário no database de gravação de metadados já com as permissões
  ```
  use mdbmmd-metadataMonitor
  db.createUser({user:"MDBIMMD01", pwd:"meta$monitor", roles:[ {role:"readWrite", db:"mdbmmd-metadataMonitor" }]})
  ```
  ou se já tiver criado o usuário no database de gravação de metadados, apenas adicione as permissões
  ```
  use mdbmmd-metadataMonitor
  db.grantRolesToUser( "MDBIMMD01", [ {role:"readWrite", db:"mdbmmd-metadataMonitor" } ] )
  ```
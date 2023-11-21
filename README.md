# openmetadata-java-connector

Connector to store in OpenMetadata some Dataitem info coming from the DigitaHub Platform. 

The data must be published on a RabbitMQ Queue in 'JSON' format.

Config the following env variables:
- **RABBITMQ_HOST** : RabbitMQ host
- **RABBITMQ_PORT** : RabbitMQ port
- **RABBITMQ_USER** : RabbitMQ user
- **RABBITMQ_PASSWORD** : RabbitMQ password
- **RABBITMQ_VIRTUALHOST** : RabbitMQ virtual host
- **SAMPLER_PROB** : sampling probability for tracing data e.g. 0.1
- **ZIPKIN_ENABLED** : enable the Zipkin tracing, [true/false]
- **ZIPKIN_ENDPOINT** : Zipkin endpoint, e.g. http://localhost:9411/
- **OPENMETADATA_HOST** : OpenMetadata host and port for SDK, e.g. http://localhost:8585/api
- **OPENMETADATA_TOKEN** : OpenMetadata JWT token
- **OPENMETADATA_DATASERVICE_NAME** : display name for DataService
- **OPENMETADATA_DATASERVICE_SQL** : DataService id for SQL table
- **OPENMETADATA_DATASERVICE_S3** : DataService id for S3 table


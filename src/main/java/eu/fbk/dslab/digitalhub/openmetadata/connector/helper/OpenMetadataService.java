package eu.fbk.dslab.digitalhub.openmetadata.connector.helper;

import org.openmetadata.client.api.DatabaseSchemasApi;
import org.openmetadata.client.api.DatabaseServicesApi;
import org.openmetadata.client.api.DatabasesApi;
import org.openmetadata.client.api.TablesApi;
import org.openmetadata.client.gateway.OpenMetadata;
import org.openmetadata.client.model.CreateDatabase;
import org.openmetadata.client.model.CreateDatabaseSchema;
import org.openmetadata.client.model.CreateDatabaseService;
import org.openmetadata.client.model.CreateTable;
import org.openmetadata.client.model.Database;
import org.openmetadata.client.model.DatabaseSchema;
import org.openmetadata.client.model.DatabaseService;
import org.openmetadata.client.model.Table;
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig;
import org.openmetadata.schema.services.connections.metadata.AuthProvider;
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import eu.fbk.dslab.digitalhub.openmetadata.connector.parser.PostgresParser;

@Service
public class OpenMetadataService implements ApplicationListener<ContextRefreshedEvent> {
	private static transient final Logger logger = LoggerFactory.getLogger(OpenMetadataService.class);
	
	@Value("${openmetadata.token}")
	private String openMetadataToken;
	
	@Value("${openmetadata.host}")
	private String openMetadataHost;
	
	@Value("${openmetadata.dataservice}")
	private String openMetadataDataService;
	
	OpenMetadata openMetadataGateway = null;
	
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		OpenMetadataJWTClientConfig openMetadataJWTClientConfig = new OpenMetadataJWTClientConfig();
		openMetadataJWTClientConfig.setJwtToken(openMetadataToken);
		
		OpenMetadataConnection config = new OpenMetadataConnection();
		config.setHostPort(openMetadataHost);
		config.setAuthProvider(AuthProvider.OPENMETADATA);
		config.setSecurityConfig(openMetadataJWTClientConfig);		
		
		openMetadataGateway = new OpenMetadata(config);
	}
	
	public void publicPostgresTable(PostgresParser data) {
		DatabaseService databaseService = createDatabaseService();
		Database database = createDatabase(databaseService, data.getDbName());
		DatabaseSchema databaseSchema = createDatabaseSchema(database, data.getDbSchema());
		createTable(databaseSchema, data.getKey(), data.getDbTable());
	}
	
	private Table createTable(DatabaseSchema databaseSchema, String key, String name) {
		CreateTable createTable = new CreateTable();
		createTable.setName(key);
		createTable.setDisplayName(name);
		createTable.setTableType(CreateTable.TableTypeEnum.REGULAR);
		createTable.setDatabaseSchema(databaseSchema.getFullyQualifiedName());
		TablesApi tablesApi = openMetadataGateway.buildClient(TablesApi.class);
		Table table = tablesApi.createOrUpdateTable(createTable);
		return table;
		//return tablesApi.getTableByID(table.getId(), "*", "all");
	}
	
	private DatabaseSchema createDatabaseSchema(Database database, String name) {
		CreateDatabaseSchema createschema = new CreateDatabaseSchema();
		createschema.setName(name);
		createschema.setDisplayName(name);
		createschema.setDatabase(database.getFullyQualifiedName());
		DatabaseSchemasApi databaseSchemasApi = openMetadataGateway.buildClient(DatabaseSchemasApi.class);
		return databaseSchemasApi.createOrUpdateDBSchema(createschema);
	}	
	
	private Database createDatabase(DatabaseService service, String name) {
	    CreateDatabase createdatabase = new CreateDatabase();
	    createdatabase.name(name);
	    createdatabase.displayName(name);
	    createdatabase.setService(service.getFullyQualifiedName());
	    DatabasesApi databasesApi = openMetadataGateway.buildClient(DatabasesApi.class);
	    return databasesApi.createOrUpdateDatabase(createdatabase);
	  }
	
	private DatabaseService createDatabaseService() {
		DatabaseServicesApi databaseServicesApi = openMetadataGateway.buildClient(DatabaseServicesApi.class);
		DatabaseService databaseService = databaseServicesApi.getDatabaseServiceByFQN(openMetadataDataService, "", "all");
		if(databaseService == null) {
			CreateDatabaseService createDatabaseService = new CreateDatabaseService();
			createDatabaseService.setServiceType(CreateDatabaseService.ServiceTypeEnum.POSTGRES);
			createDatabaseService.setName(openMetadataDataService);
			createDatabaseService.setDisplayName(openMetadataDataService);
			databaseService = databaseServicesApi.createOrUpdateDatabaseService(createDatabaseService);
		}
		return databaseService;
	}
	
//	private EntityReference buildEntityReference(String type, UUID id, String name) {
//		EntityReference entityReference = new EntityReference();
//		entityReference.setId(id);
//		entityReference.setName(name);
//		entityReference.setType(type);
//		return entityReference;
//	}	
	
}

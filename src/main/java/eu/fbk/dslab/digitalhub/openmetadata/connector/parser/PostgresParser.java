package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;

public class PostgresParser {
	private String project;
	private String name;
	private String version;
	private String path;
	private String key;
	private String source;
	private String dbName;
	private String dbSchema;
	private String dbTable;
	
	public PostgresParser(JsonNode rootNode) {
		project = rootNode.get("metadata").get("project").asText();
		name = rootNode.get("metadata").get("name").asText();
		version = rootNode.get("metadata").get("version").asText();
		source = rootNode.get("spec").get("key").asText();
		key = project + "_" + name;
		path = rootNode.get("spec").get("path").asText();
		String[] strings = StringUtils.remove(path, "sql://").split("/");
		if(strings.length == 3) {
			dbName = strings[0];
			dbSchema = strings[1];
			dbTable = strings[2];
		} else {
			dbName = strings[0];
			dbSchema = "public";
			dbTable = strings[1];
		}
	}

	public String getProject() {
		return project;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getDbSchema() {
		return dbSchema;
	}

	public void setDbSchema(String dbSchema) {
		this.dbSchema = dbSchema;
	}

	public String getDbTable() {
		return dbTable;
	}

	public void setDbTable(String dbTable) {
		this.dbTable = dbTable;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
}

package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class DataItemParser {
	String project;
	String name;
	String version;
	String path;
	String key;
	String source;
	String dbName;
	String dbSchema;
	String dbTable;
	List<TableColumn> columns = new ArrayList<>();
	
	public abstract void parseItem(JsonNode rootNode);
	
	void fillColumns(JsonNode rootNode) {
		Map<String, JsonNode> previewMap = new HashMap<>();
		if(rootNode.get("status").hasNonNull("preview")) {
			Iterator<JsonNode> previewsNode = rootNode.get("status").get("preview").elements();
			while(previewsNode.hasNext()) {
				JsonNode previewNode = (JsonNode) previewsNode.next();
				previewMap.put(previewNode.get("name").asText(), previewNode);
			}
		}
		if(rootNode.get("spec").hasNonNull("schema")) {
			Iterator<JsonNode> columnsNode = rootNode.get("spec").get("schema").elements();
			while(columnsNode.hasNext()) {
				JsonNode columnNode = (JsonNode) columnsNode.next();
				String name = columnNode.get("name").asText();
				String type = columnNode.get("type").asText();
				TableColumn column = new TableColumn(name, PostgresType.getDataType(type));
				if(previewMap.containsKey(name)) {
					JsonNode previewNode = previewMap.get(name);
					Iterator<JsonNode> elements = previewNode.get("value").elements();
					while(elements.hasNext()) {
						JsonNode node = elements.next();
						column.getPreview().add(node.asText());
					}
				}
				columns.add(column);
			}			
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
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
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
	public List<TableColumn> getColumns() {
		return columns;
	}
	public void setColumns(List<TableColumn> columns) {
		this.columns = columns;
	}

}

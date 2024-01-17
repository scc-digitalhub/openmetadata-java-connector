package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;

public class PostgresParser extends DataItemParser {
	
	@Override
	public void parseItem(JsonNode rootNode) {
		if(rootNode.get("metadata").hasNonNull("project")) {
			project = rootNode.get("metadata").get("project").asText();
		} else {
			project = rootNode.get("project").asText();
		}
		if(rootNode.get("metadata").hasNonNull("name")) {
			name = rootNode.get("metadata").get("name").asText();
		} else {
			name = rootNode.get("name").asText();
		}
		if(rootNode.get("metadata").hasNonNull("version")) {
			version = rootNode.get("metadata").get("version").asText();
		} else {
			version = rootNode.get("id").asText();
		}
		source = rootNode.get("spec").get("key").asText();
		key = project + "_" + name;
		path = rootNode.get("spec").get("path").asText();
		String[] strings = StringUtils.remove(path, "sql://").split("/");
		if(strings.length == 3) {
			dbName = strings[0];
			dbSchema = strings[1];
			dbTable = new String(name);
		} else {
			dbName = strings[0];
			dbSchema = "public";
			dbTable = new String(name);
		}
		fillColumns(rootNode);
	}

}

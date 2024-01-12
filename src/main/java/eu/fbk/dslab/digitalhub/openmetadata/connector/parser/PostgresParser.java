package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;

public class PostgresParser extends DataItemParser {
	
	@Override
	public void parseItem(JsonNode rootNode) {
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
			dbTable = rootNode.get("metadata").get("name").asText();
		} else {
			dbName = strings[0];
			dbSchema = "public";
			dbTable = rootNode.get("metadata").get("name").asText();
		}
		fillColumns(rootNode);
	}

}

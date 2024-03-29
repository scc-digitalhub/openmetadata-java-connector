package eu.fbk.dslab.digitalhub.openmetadata.connector.parser;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;

public class S3Parser extends DataItemParser {
	
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
		String[] strings = StringUtils.remove(path, "s3://").split("/");
		dbName = strings[0];
		dbTable = strings[strings.length-1];
		dbSchema = "";
		for(int i=1; i<strings.length-1; i++) {
			dbSchema += "/" + strings[i];
		}
		fillColumns(rootNode);
	}
		
}

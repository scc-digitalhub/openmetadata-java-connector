package eu.fbk.dslab.digitalhub.openmetadata.connector.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import eu.fbk.dslab.digitalhub.openmetadata.connector.config.RabbitConf;
import eu.fbk.dslab.digitalhub.openmetadata.connector.helper.OpenMetadataService;
import eu.fbk.dslab.digitalhub.openmetadata.connector.parser.DataItemParser;
import eu.fbk.dslab.digitalhub.openmetadata.connector.parser.PostgresParser;
import eu.fbk.dslab.digitalhub.openmetadata.connector.parser.S3Parser;

@Component
public class MessageQueueManager {
	private static transient final Logger logger = LoggerFactory.getLogger(MessageQueueManager.class);
	
	@Autowired
	RabbitTemplate rabbitTemplate;
	
	@Autowired
	OpenMetadataService openMetadataService;
	
	ObjectMapper mapper = new ObjectMapper();

	@RabbitListener(queues = RabbitConf.openMetadataEvent, concurrency = "5")
	public void openMetadataEventCallback(Message delivery) {
        try {
            String json = new String(delivery.getBody(), "UTF-8");
            logger.info("openMetadataEventCallback:" + json);
            JsonNode rootNode = mapper.readTree(json);
            String kind = rootNode.get("kind").asText();
            switch (kind) {
			case "dataitem": {
				String path = rootNode.get("spec").get("path").asText();
				String protocol = path.split(":")[0];
				DataItemParser parser = null;
				if(protocol.equalsIgnoreCase("sql")) {
					parser = new PostgresParser();
				} else if(protocol.equalsIgnoreCase("s3")) {
					parser = new S3Parser();
				}
				parser.parseItem(rootNode);
				openMetadataService.publicTable(parser);
				break;
			}
			default:
				throw new IllegalArgumentException("Unexpected value: " + kind);
			} 
        } catch (Exception e) {
            logger.warn(String.format("openMetadataEventCallback:%s", e.getMessage()));
        }
	}
	
}

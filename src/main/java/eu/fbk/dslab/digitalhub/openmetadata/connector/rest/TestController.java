package eu.fbk.dslab.digitalhub.openmetadata.connector.rest;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import eu.fbk.dslab.digitalhub.openmetadata.connector.config.RabbitConf;

@RestController
public class TestController {
	@Autowired
	RabbitTemplate rabbitTemplate;

	@PostMapping("/test/postgres")
	public void testPostgresMsg(@RequestBody String msg) {
		rabbitTemplate.convertAndSend(RabbitConf.openMetadataEvent, msg);		
	}
	
	@PostMapping("/test/s3")
	public void testS3Msg(@RequestBody String msg) {
		rabbitTemplate.convertAndSend(RabbitConf.openMetadataEvent, msg);		
	}

}

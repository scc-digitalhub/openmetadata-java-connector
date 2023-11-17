package eu.fbk.dslab.digitalhub.openmetadata.connector;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class OpenMetadataConnectorApplicationTests {
//	@Autowired
//	ResourceLoader resourceLoader;
//	@Autowired
//	RabbitTemplate rabbitTemplate;

	@Test
	void contextLoads() {
	}
	
//	void testPostgresMsg() throws IOException {
//		String msg = resourceLoader.getResource("classpath:dataitem_postgres.json").getContentAsString(Charset.forName("UTF-8"));
//		rabbitTemplate.convertAndSend(RabbitConf.openMetadataEvent, msg);
//	}
}

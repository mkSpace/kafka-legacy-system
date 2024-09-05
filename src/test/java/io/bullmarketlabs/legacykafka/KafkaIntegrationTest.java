package io.bullmarketlabs.legacykafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaIntegrationTest {

    @Autowired
    KafkaAdmin kafkaAdmin;

    private AdminClient adminClient;

    @BeforeEach
    public void setup() {
        adminClient = AdminClient.create(kafkaAdmin.getConfig());
        System.out.println("setup!!" + adminClient);
    }

    @AfterEach
    public void tearDown() {
        if(adminClient != null) {
            adminClient.close();
        }
        System.out.println("Tear Down!!");
    }

    @Test
    public void testCreateTopic() throws ExecutionException, InterruptedException {
        setup();
        NewTopic newTopic = new NewTopic("new_test_topic", 1, (short) 1);
        KafkaFuture<Void> future = adminClient.createTopics(Collections.singletonList(newTopic)).all();
        future.get();

        Assertions.assertThat(adminClient.listTopics().names().get().contains("new_test_topic")).isTrue();
    }

}

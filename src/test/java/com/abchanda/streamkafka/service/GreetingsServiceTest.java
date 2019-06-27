package com.abchanda.streamkafka.service;

import com.abchanda.streamkafka.model.Greetings;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.cloud.stream.test.binder.MessageCollector;
import org.springframework.messaging.Message;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class GreetingsServiceTest {

    @Autowired
    private Processor greetingsProcessor;

    @Autowired
    private MessageCollector messageCollector;

    @Autowired
    private GreetingsService greetingsService;

    @Test
    @SuppressWarnings("unchecked")
    public void testSendGreeting() throws Exception {

        Long currentTimeStamp = System.currentTimeMillis();
        Greetings greetings = Greetings.builder().message("Test Message").timestamp(currentTimeStamp).build();

        greetingsService.sendGreeting(greetings);

        Message<?> received = messageCollector.forChannel(greetingsProcessor.output())
                .poll();
        assertNotNull(received.getPayload());

        ObjectMapper objectMapper = new ObjectMapper();
        Greetings msgGreetings = objectMapper.readValue(received.getPayload().toString(), Greetings.class);

        assertEquals("Test Message", msgGreetings.getMessage());
    }
}
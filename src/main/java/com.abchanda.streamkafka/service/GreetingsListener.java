package com.abchanda.streamkafka.service;

import com.abchanda.streamkafka.model.Greetings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingsListener {
    @StreamListener(Processor.INPUT)
    public void handleGreetings(@Payload Greetings greetings) {
        log.info("Received greetings: {}", greetings);
    }
}

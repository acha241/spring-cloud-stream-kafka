package com.abchanda.streamkafka.config;

import com.abchanda.streamkafka.stream.GreetingsStreams;
import org.springframework.cloud.stream.annotation.EnableBinding;

@EnableBinding(GreetingsStreams.class)
public class StreamsConfig {
}

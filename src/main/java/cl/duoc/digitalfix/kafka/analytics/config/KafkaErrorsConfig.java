package cl.duoc.digitalfix.kafka.analytics.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorsConfig {

    private static final Logger log =
            LoggerFactory.getLogger(KafkaErrorsConfig.class);

    @Bean
    public CommonErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafka) {

        DeadLetterPublishingRecoverer recuperador =
                new DeadLetterPublishingRecoverer(
                        kafka,
                        (registro, ex) -> {
                            log.error(
                                    "Kafka DLT: topic={}, partition={}, offset={}, causa={}",
                                    registro.topic(),
                                    registro.partition(),
                                    registro.offset(),
                                    ex.toString()
                            );

                            return new TopicPartition(
                                    registro.topic() + ".DLT",
                                    registro.partition()
                            );
                        }
                );

        DefaultErrorHandler handler = new DefaultErrorHandler(
                recuperador,
                new FixedBackOff(1000L, 2L)
        );

        handler.addNotRetryableExceptions(
                IllegalArgumentException.class
        );

        return handler;
    }
}
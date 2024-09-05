package io.bullmarketlabs.legacykafka.consumer;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerRunner implements ApplicationRunner {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final String topicName;

    private final Scheduler scheduler = Schedulers.newThread();
    private final CompositeDisposable disposables = new CompositeDisposable();

    @Override
    public void run(ApplicationArguments args) {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        Disposable disposable = Flowable.interval(100, TimeUnit.MILLISECONDS)
                .subscribeOn(scheduler)
                .subscribe(data -> {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Consume new record: {}", record);
                    }
                });
        disposables.add(disposable);
    }

    @PreDestroy
    public void destroy() {
        disposables.clear();
    }
}

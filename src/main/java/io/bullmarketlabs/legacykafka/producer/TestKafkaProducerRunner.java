package io.bullmarketlabs.legacykafka.producer;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Scheduler;
import io.reactivex.rxjava3.disposables.CompositeDisposable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
@RequiredArgsConstructor
public class TestKafkaProducerRunner implements ApplicationRunner {
    private final String topicName;
    private final KafkaProducerService kafkaProducerService;

    private final Scheduler scheduler = Schedulers.newThread();
    private final CompositeDisposable disposables = new CompositeDisposable();

    @Override
    public void run(ApplicationArguments args) {
        Disposable disposable = Flowable.interval(1, TimeUnit.SECONDS)
                .observeOn(scheduler)
                .subscribe(data -> kafkaProducerService.send(topicName, String.valueOf(data)));
        disposables.add(disposable);
    }

    @PreDestroy
    public void destroy() {
        disposables.clear();
    }
}

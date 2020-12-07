package com.tango.stream.manager.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.ReloadingFileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.FileBasedBuilderParameters;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.reloading.PeriodicReloadingTrigger;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Service
public class ConfigurationService implements Supplier<ImmutableConfiguration> {
    private final Configuration mainConfiguration;
    private final Configuration updatingConfiguration;
    private volatile ImmutableConfiguration reloadedConfiguration = null;
    private volatile ImmutableConfiguration compositeConfiguration;
    private final ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> configurationBuilder;
    private final PeriodicReloadingTrigger trigger;
    private final AtomicInteger errorCounter = new AtomicInteger();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new BasicThreadFactory.Builder()
            .namingPattern("configuration-thread-%d")
            .daemon(true)
            .build());
    private final long reloadPeriodMillis;

    public ConfigurationService(ConfigurableEnvironment env,
                                @Value("${reload.config.location}") String path,
                                @Value("${reload.config.period.millis:5000}") long reloadPeriodMillis) {
        Map<String, String> propertyMap = StreamSupport.stream(env.getPropertySources().spliterator(), false)
                .filter(ps -> ps instanceof EnumerablePropertySource)
                .map(ps -> ((EnumerablePropertySource<?>) ps).getPropertyNames())
                .flatMap(Arrays::stream)
                .distinct()
                .collect(Collectors.toMap(Function.identity(), env::getProperty));
        this.mainConfiguration = new MapConfiguration(propertyMap);
        this.updatingConfiguration = new MapConfiguration(new ConcurrentHashMap<>());
        this.compositeConfiguration = new CompositeConfiguration(mainConfiguration, Collections.singleton(updatingConfiguration));

        ReloadingFileBasedConfigurationBuilder<PropertiesConfiguration> builder;
        try {
            FileBasedBuilderParameters parameters = new Parameters().fileBased().setFile(new File(path)).setListDelimiterHandler(new DefaultListDelimiterHandler(','));
            builder = new ReloadingFileBasedConfigurationBuilder<>(PropertiesConfiguration.class).configure(parameters);
        } catch (Exception e) {
            log.error("Can't setup reloadable properties, path: {}", path, e);
            builder = null;
        }

        this.configurationBuilder = builder;
        this.reloadPeriodMillis = reloadPeriodMillis;
        this.trigger = this.configurationBuilder != null ? new PeriodicReloadingTrigger(configurationBuilder.getReloadingController(),
                null, reloadPeriodMillis, TimeUnit.MILLISECONDS) : null;
    }

    @PostConstruct
    private void init() {
        if (trigger != null) {
            trigger.start();
            executorService.scheduleWithFixedDelay(this::get, reloadPeriodMillis, reloadPeriodMillis, TimeUnit.MILLISECONDS);
            log.info("reloading trigger started");
        }
    }

    @Override
    public ImmutableConfiguration get() {
        ImmutableConfiguration compositeConfiguration = this.compositeConfiguration;
        PropertiesConfiguration newConfiguration;
        try {
            newConfiguration = this.configurationBuilder.getConfiguration();
        } catch (Exception e) {
            if (this.errorCounter.getAndIncrement() % 100 == 0) {
                log.error("Can't get reloading configuration", e);
            }
            return compositeConfiguration;
        }

        if (newConfiguration == null) {
            return compositeConfiguration;
        }

        ImmutableConfiguration lastReloaded = this.reloadedConfiguration;
        if (lastReloaded != null && lastReloaded == newConfiguration) {
            return compositeConfiguration;
        }

        // cache composite configuration
        compositeConfiguration = new CompositeConfiguration(mainConfiguration, Arrays.asList(updatingConfiguration, newConfiguration));
        this.reloadedConfiguration = newConfiguration;
        this.compositeConfiguration = compositeConfiguration;

        if (lastReloaded != null) { // check for property changes
            Stream.concat(keys(lastReloaded), keys(newConfiguration))
                    .distinct()
                    .forEach(key -> {
                        Object oldValue = lastReloaded.getProperty(key);
                        Object newValue = newConfiguration.getProperty(key);
                        if (!Objects.equals(oldValue, newValue)) {
                            log.info("config change for key: {} from : {}, to: {}", key, oldValue, newValue);
                        }
                    });
        }
        return compositeConfiguration;
    }

    public void update(Consumer<Configuration> configurationConsumer) {
        configurationConsumer.accept(updatingConfiguration);
    }

    private Stream<String> keys(ImmutableConfiguration lastReloaded) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(lastReloaded.getKeys(),
                Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE), false);
    }

    @PreDestroy
    private void shutdown() {
        executorService.shutdownNow();
    }
}

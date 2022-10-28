package com.wyy.tool.common;

import java.util.concurrent.TimeUnit;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.JvmAttributeGaugeSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;

public class MetricsSystem {
  public static final MetricRegistry METRIC_REGISTRY;

  public static ConsoleReporter METRIC_REPOTER;

  static {
    METRIC_REGISTRY = new MetricRegistry();
//    METRIC_REGISTRY.registerAll(new JvmAttributeGaugeSet());
//    METRIC_REGISTRY.registerAll(new GarbageCollectorMetricSet());
//    METRIC_REGISTRY.registerAll(new MemoryUsageGaugeSet());
//    METRIC_REGISTRY.registerAll(new ClassLoadingGaugeSet());
//    METRIC_REGISTRY.registerAll(new CachedThreadStatesGaugeSet(5, TimeUnit.SECONDS));
//    METRIC_REGISTRY.registerAll(new OperationSystemGaugeSet());
    METRIC_REPOTER = ConsoleReporter.forRegistry(METRIC_REGISTRY)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
  }

  public static Meter meter(Class<?> klass, String... names) {
    return METRIC_REGISTRY.meter(MetricRegistry.name(klass, names));
  }

  public static Timer timer(Class<?> klass, String... names) {
    return METRIC_REGISTRY.timer(MetricRegistry.name(klass, names));
  }

  public static void startReport() {
    METRIC_REPOTER = ConsoleReporter.forRegistry(METRIC_REGISTRY)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    METRIC_REPOTER.start(5, TimeUnit.SECONDS);
  }

  public static void stopReport() {
    if (METRIC_REPOTER!= null) {
      METRIC_REPOTER.stop();
      METRIC_REPOTER.close();
    }
  }
}

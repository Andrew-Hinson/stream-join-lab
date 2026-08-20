FROM apache/kafka:4.3.1
USER root
ADD https://github.com/prometheus/jmx_exporter/releases/download/1.6.0/jmx_prometheus_javaagent-1.6.0.jar /opt/jmx/jmx_prometheus_javaagent.jar
RUN chmod 755 /opt/jmx && chmod 644 /opt/jmx/jmx_prometheus_javaagent.jar
USER appuser

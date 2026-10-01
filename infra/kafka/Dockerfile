# Официальный образ Apache Kafka (KRaft, без ZooKeeper) + утилиты для имитации сетевых сбоев.
# iproute2 даёт команду `tc` (traffic control) — ею control-center добавляет задержку,
# потерю пакетов и изоляцию брокера прямо внутри его сетевого namespace.
FROM apache/kafka:4.1.0

USER root
RUN apk add --no-cache iproute2

# Скрипт создания топиков (его запускает одноразовый контейнер kafka-init).
COPY create-topics.sh /opt/lab/create-topics.sh
RUN sed -i 's/\r$//' /opt/lab/create-topics.sh && chmod 755 /opt/lab/create-topics.sh

USER appuser

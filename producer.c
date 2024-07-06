#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

#define NUM_MESSAGES 1000000


int main() {
    char errstr[512];
    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr));

    rd_kafka_conf_set(conf, "compression.codec", "gzip", NULL, 0);

    // Set the 'batch.num.messages' configuration parameter to '10000'
    rd_kafka_conf_set(conf, "batch.num.messages", "10000", NULL, 0);

    // Set the 'queue.buffering.max.ms' configuration parameter to '1000'
    rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1000", NULL, 0);

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        return 1;
    }

    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, "numbers", NULL);
    if (rkt == NULL) {
        fprintf(stderr, "%% Failed to create topic object: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return 1;
    }

    for (int i = 0; i < NUM_MESSAGES; i++) {
        int num = rand() % 1000000 + 1;

        if (rd_kafka_produce(rkt, num % 8, RD_KAFKA_MSG_F_COPY,
                            &num, sizeof(num), NULL, 0, NULL) == -1) {
            fprintf(stderr, "%% Failed to produce to topic %s: %s\n",
                    rd_kafka_topic_name(rkt), rd_kafka_err2str(rd_kafka_last_error()));
        }
        if(i % 1000 == 0) {
            rd_kafka_flush(rk, 10*1000);
        }
    }

    rd_kafka_flush(rk, 10*1000);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    printf("done");
    return 0;
}
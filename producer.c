#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>

#define NUM_MESSAGES 1000 * 1000 * 1000
#define NUM_THREADS 4
#define NUM_PARTITIONS 50
#define BATCH_SIZE 10000

/**
 * Produces messages with in the thread context.
 * Each message is a collection of random numbers.
 */
void *produce_messages(void *args) {
    char errstr[512];
    int batch[BATCH_SIZE];

    int * tid = (int *) args;
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", 
        "localhost:9092,localhost:9093,localhost:9094,localhost:9095,localhost:9096,localhost:9097,localhost:9098", 
        errstr, sizeof(errstr));

    rd_kafka_conf_set(conf, "batch.num.messages", "10000", NULL, 0);
    rd_kafka_conf_set(conf, "acks", "0", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1000", NULL, 0);

    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
        return NULL;
    }

    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, "numbers", NULL);
    if (rkt == NULL) {
        fprintf(stderr, "%% Failed to create topic object: %s\n",
                rd_kafka_err2str(rd_kafka_last_error()));
        rd_kafka_destroy(rk);
        return NULL;
    }

    // this is where the boiler plate ends and we start producing the numbers 
    // and sending them to Kafka
    for (int i = 0, k = NUM_MESSAGES / NUM_THREADS, flush =0; i < k;) {
        int part = rand() % NUM_PARTITIONS;
        int j;
        for (j = 0; j < BATCH_SIZE; ++j) {
            batch[j] = rand() % 1000000 + 1;
            if(batch[j] < 0 || batch[j] > 1000000) {
                printf("WTF %d\n", batch[j]);
            }
        }
        i += j;
        if (rd_kafka_produce(rkt, part, RD_KAFKA_MSG_F_COPY,
                            batch, j * sizeof(int), NULL, 0, NULL) == -1) {
            fprintf(stderr, "%% Failed to produce to topic %s: %s\n",
                    rd_kafka_topic_name(rkt), rd_kafka_err2str(rd_kafka_last_error()));
        }
        flush++;
        if(flush == 200) {
            flush = 0;
            rd_kafka_flush(rk, 10*1000);
        }
    }


    rd_kafka_flush(rk, 10*1000);
    rd_kafka_topic_destroy(rkt);
    rd_kafka_destroy(rk);

    pthread_exit(NULL);
}

int main() {

    pthread_t threads[NUM_THREADS];
    long t;
    int ta[NUM_THREADS];


    for(t=0; t<NUM_THREADS; t++){
        ta[t] = t;
        int rc = pthread_create(&threads[t], NULL, produce_messages, (void *)&ta[t]);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }

    for(t=0; t<NUM_THREADS; t++){
        pthread_join(threads[t], NULL);
    }


    printf("done");
    return 0;
}
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>

#define NUM_MESSAGES 1000 * 1000 * 1000
#define NUM_THREADS 50

void * consume_messages(void *args) {
    int * counts = (int *) malloc(sizeof(int) * 1000000);
    memset(counts, 0, sizeof(int) * 1000000);
    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // Set the 'bootstrap.servers' configuration parameter
    rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094,localhost:9095", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", "mygroup.1", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        return NULL;
    }

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, "numbers", RD_KAFKA_PARTITION_UA);
    

    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
    if (err) {
        fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                topics->cnt, rd_kafka_err2str(err));
        return NULL;
    }

    
    for(int consumed=0, j=NUM_MESSAGES / NUM_THREADS; consumed < j ; ) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (rkmessage) {
            int *nums = (int *)rkmessage->payload;
            int num_count = rkmessage->len / sizeof(int);
            for (int i = 0; i < num_count; ++i) {
                int num = nums[i];
                counts[num]++;
                consumed++;
                if(consumed % 10000 == 0) {
                    printf("Consumed %d messages\n", consumed);
                }
            }
            rd_kafka_message_destroy(rkmessage);
        }
    }

    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_destroy(rk);

    pthread_exit(counts);
    return NULL;
}

int main() {
    int counts[1000000] = {0};  // Initialize all counts to 0
    int *result;
    pthread_t threads[NUM_THREADS];


    for(int t=0; t<NUM_THREADS; t++){
        int rc = pthread_create(&threads[t], NULL, consume_messages, NULL);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }


    for(int t=0; t<NUM_THREADS; t++){
        pthread_join(threads[t], (void **) &result);
        for(int i = 0 ; i < 1000000 ; i++){
            counts[i] += ((int *)result)[i];
        }
    }

    FILE * fp = fopen("counts.txt","w");
    for (int i = 0; i < 1000000; i++) {
        if (counts[i] > 0) {
            fprintf(fp, "%d: %d\n", i, counts[i]);
        }
    }
    fclose(fp);


    return 0;
}
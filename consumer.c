#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>
#include <pthread.h>

#define NUM_MESSAGES 1000 * 1000 * 1000
#define NUM_THREADS 50

pthread_barrier_t barrier;

/**
 * Consumes messages from the Kafka topic 'numbers'.
 * How many times each number is seen will be saved into an integer array. 
 * When the thread has completed the counts will be returned, where it will
 * be available to the caller through pthread_join
 */
void * consume_messages(void *args) {
    int * counts = (int *) malloc(sizeof(int) * 1000000);
    if(counts == NULL) {
        printf("Could not allocate memory\n");
        return NULL;
    }
    memset(counts, 0, sizeof(int) * 1000000);
    char errstr[512];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // use multiple brokers to increase throughput
    rd_kafka_conf_set(conf, "bootstrap.servers", 
        "localhost:9092,localhost:9093,localhost:9094,localhost:9095,localhost:9096,localhost:9097,localhost:9098",
         errstr, sizeof(errstr));

    rd_kafka_conf_set(conf, "group.id", "mygroup.17", errstr, sizeof(errstr));
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

    // wait until everyone is ready. That way we are able to cut down on a lot of 
    // rebalancing
    pthread_barrier_wait(&barrier);

    // this is where the kafka subscription boiler plate ends and the work begins.
    int missed = 0;
    int consumed=0;
    while(1) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (rkmessage) {
            int *nums = (int *)rkmessage->payload;
            int num_count = rkmessage->len / sizeof(int);
            for (int i = 0; i < num_count; ++i) {
                int num = nums[i];
                if(num > 1000000 || num < 0) {
                    printf("MAYDAY %d\n", num);
                    return NULL;
                }
                counts[num]++;
                // if(consumed % 1000000 == 0) {
                //      printf("Consumed %d messages\n", consumed);
                // }
            }
            consumed += num_count;
            rd_kafka_message_destroy(rkmessage);
            missed = 0;
        }
        else {
            missed++;
            if(missed == 6) {
                // looks like this partition is exhuasted
                //printf("Exhuasted\n");
                break;
            }
        }
    }

    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_destroy(rk);

    // printf("I ate %d numbers\n", consumed);
    pthread_exit(counts);
    return NULL;
}

int main() {
    int counts[1000000] = {0};  // Initialize all counts to 0
    int *result;
    pthread_t threads[NUM_THREADS];

    pthread_barrier_init(&barrier, NULL, NUM_THREADS);

    for(int t=0; t<NUM_THREADS; t++){
        int rc = pthread_create(&threads[t], NULL, consume_messages, NULL);
        if (rc){
            printf("ERROR; return code from pthread_create() is %d\n", rc);
            exit(-1);
        }
    }

    // Wait for all threads to complete and merge their results
    for(int t=0; t<NUM_THREADS; t++){
        pthread_join(threads[t], (void **) &result);
        for(int i = 0 ; i < 1000000 ; i++){
            counts[i] += ((int *)result)[i];
        }
        free(result);
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
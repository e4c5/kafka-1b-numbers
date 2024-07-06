#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <librdkafka/rdkafka.h>

#define NUM_MESSAGES 1000000

int main() {
    char errstr[512];
    int counts[1000000] = {0};  // Initialize all counts to 0


    rd_kafka_conf_t *conf = rd_kafka_conf_new();

    // Set the 'bootstrap.servers' configuration parameter
    rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", "mygroup", errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", errstr, sizeof(errstr));
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

    if (rk == NULL) {
        fprintf(stderr, "%% Failed to create new consumer: %s\n", errstr);
        return 1;
    }

    rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, "numbers", RD_KAFKA_PARTITION_UA);
    

    rd_kafka_resp_err_t err = rd_kafka_subscribe(rk, topics);
    if (err) {
        fprintf(stderr, "%% Failed to subscribe to %d topics: %s\n",
                topics->cnt, rd_kafka_err2str(err));
        return 1;
    }

    
    for(int consumed=0; consumed < NUM_MESSAGES ; consumed++) {
        rd_kafka_message_t *rkmessage;
        rkmessage = rd_kafka_consumer_poll(rk, 1000);
        if (rkmessage) {
            
            int num = *(int *)rkmessage->payload;
            counts[num]++;
            if(consumed % 1000 == 0) {
                printf("Consumed %d messages\n", consumed);
            }
            
            rd_kafka_message_destroy(rkmessage);
        }
    }

    // Print the counts
    for (int i = 0; i < 1000000; i++) {
        if (counts[i] > 0) {
         //   printf("%d: %d\n", i, counts[i]);
        }
    }

    printf("Done\n");
    rd_kafka_topic_partition_list_destroy(topics);
    rd_kafka_destroy(rk);

    return 0;
}
// ConsoleApplication1.cpp : This file contains the 'main' function. Program execution begins and ends there.
//

#include <iostream>
#include <csignal>
#include <librdkafka/rdkafkacpp.h>

static volatile sig_atomic_t run = 1;

static void sigterm(int sig) {
    run = 0;
}

class FormatedDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message& message) {
        /* If message.err() is non-zero the message delivery failed permanently
         * for the message. */
        if (message.err())
            std::cerr << "% Message delivery failed: " << message.errstr()
            << std::endl;
        else
            std::cerr << "% Message delivered to topic " << message.topic_name()
            << " [" << message.partition() << "] at offset "
            << message.offset() << std::endl;
    }
};

int main()
{
    
    std::string brokers = "localhost:9092";
    std::string topic = "test";
    std::string errstr = "";

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);

    if(conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }
    
    FormatedDeliveryReportCb deliveryReportCallBack;

    if (conf->set("dr_cb", &deliveryReportCallBack, errstr) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    delete conf;

    std::string line = "Hello";
        if (line.empty()) 
        {
            producer->poll(0);
        }

        RdKafka::ErrorCode err = producer->produce(
            topic,
            RdKafka::Topic::PARTITION_UA,
            RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
            const_cast<char*>(line.c_str()), line.size(),
            NULL, 0,
            0,
            NULL,
            NULL);

        if (err != RdKafka::ERR_NO_ERROR) 
        {
            std::cerr << "% Failed to produce to topic " << topic << ": "
                << RdKafka::err2str(err) << std::endl;

            if (err == RdKafka::ERR__QUEUE_FULL) {              
                producer->poll(1000 /*block for max 1000ms*/);
            }

        }
        else {
            std::cerr << "% Enqueued message (" << line.size() << " bytes) "
                << "for topic " << topic << std::endl;
        }

        producer->poll(0);

    std::cerr << "% Flushing final messages..." << std::endl;
    producer->flush(10 * 1000 /* wait for max 10 seconds */);

    if (producer->outq_len() > 0)
        std::cerr << "% " << producer->outq_len()
        << " message(s) were not delivered" << std::endl;

    delete producer;

    return 0;
}
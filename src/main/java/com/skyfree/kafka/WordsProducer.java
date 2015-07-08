package com.skyfree.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/7/8 16:29
 */
public class WordsProducer {
    private static String METAMORPHOSIS_OPENING_PARA = "One morning, when Gregor Samsa woke from troubled dreams, "
            + "he found himself transformed in his bed into a horrible "
            + "vermin. He lay on his armour-like back, and if he lifted "
            + "his head a little he could see his brown belly, slightly "
            + "domed and divided by arches into stiff sections.";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("metadata.broker.list", "skyfree3:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (String word : METAMORPHOSIS_OPENING_PARA.split("\\s")) {
            KeyedMessage<String, String> message = new KeyedMessage<String, String>("words_topic", word);
            producer.send(message);
        }

        System.out.println("sent data completely!");

        producer.close();
    }
}

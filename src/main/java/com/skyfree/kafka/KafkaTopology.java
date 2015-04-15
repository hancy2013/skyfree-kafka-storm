package com.skyfree.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/15 下午2:33
 */
public class KafkaTopology {
    public static void main(String[] args) {
        ZkHosts zkHosts = new ZkHosts("l-skyfree.ops.dev.cn0.qunar.com:2181");
        SpoutConfig config = new SpoutConfig(zkHosts, "words_topic", "", "skyfree_group");
        config.scheme = new SchemeAsMultiScheme(new StringScheme());
        config.forceFromStart = true;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka_spout", new KafkaSpout(config), 1);
        builder.setBolt("sentence_bolt", new SentenceBolt(), 1).globalGrouping("kafka_spout");
        builder.setBolt("print_bolt", new PrintBolt(), 1).globalGrouping("sentence_bolt");

        LocalCluster cluster = new LocalCluster();
        Config stormConfig = new Config();
        cluster.submitTopology("kafka_topology", stormConfig, builder.createTopology());
        try {
            System.out.println("Waiting to consume from kafka");
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted exception:" + e);
        }

        cluster.killTopology("kafka_topology");
        cluster.shutdown();

    }
}

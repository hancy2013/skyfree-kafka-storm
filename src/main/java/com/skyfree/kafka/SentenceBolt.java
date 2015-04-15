package com.skyfree.kafka;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Copyright @ 2015 OPS
 * Author: tingfang.bao <mantingfangabc@163.com>
 * DateTime: 15/4/15 下午2:26
 */
public class SentenceBolt extends BaseBasicBolt {
    private List<String> words = new ArrayList<String>();

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);

        if (StringUtils.isBlank(word)) {
            return;
        }
        System.out.println("Received Word:" + word);

        words.add(word);

        if (word.endsWith(".")) {
            basicOutputCollector.emit(ImmutableList.of((Object) StringUtils.join(words, ' ')));
            words.clear();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}

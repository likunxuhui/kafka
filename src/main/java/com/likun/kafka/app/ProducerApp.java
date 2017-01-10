package com.likun.kafka.app;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class ProducerApp {

	private final Producer<String, String> producer;
    public final static String TOPIC = "test";

    private ProducerApp(){
        Properties props = new Properties();
        //ï¿½Ë´ï¿½ï¿½ï¿½ï¿½Ãµï¿½ï¿½ï¿½kafkaï¿½Ä¶Ë¿ï¿½
        props.put("metadata.broker.list", "127.0.0.1:9092");

        //ï¿½ï¿½ï¿½ï¿½valueï¿½ï¿½ï¿½ï¿½ï¿½Ğ»ï¿½ï¿½ï¿½
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //ï¿½ï¿½ï¿½ï¿½keyï¿½ï¿½ï¿½ï¿½ï¿½Ğ»ï¿½ï¿½ï¿½
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        props.put("request.required.acks","-1");

        producer = new Producer<String, String>(new ProducerConfig(props));
    }

    void produce() {
        int messageNo = 0;
        final int COUNT = 10;

        while (messageNo < COUNT) {
            String key = String.valueOf(messageNo);
            String data = "hello kafka message " + key;
            producer.send(new KeyedMessage<String, String>(TOPIC, key ,data));
            System.out.println(data);
            messageNo ++;
        }
        System.out.println("æ¶ˆæ¯å‘é?æˆåŠŸã?‹ã?‹ã??");
    }

    public static void main( String[] args )
    {
        new ProducerApp().produce();
    }
}

package com.atguigu.rabbitmq.eight;

import com.atguigu.rabbitmq.utils.RabbitMQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

/*
 * 死信队列实战
 * 消费者02
 * */
public class Consumer02 {

    //死信队列名称
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws  Exception{
        Channel channel = RabbitMQUtils.getChannel();
        System.out.println("等待接收消息......");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("Consumer02接收的消息是：" + new String(message.getBody()));
        };

        channel.basicConsume(DEAD_QUEUE,true,deliverCallback,consumerTag->{});
    }
}

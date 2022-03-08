package com.atguigu.rabbitmq.one;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
    //队列名称
    public static final  String QUEUE_NAME = "hello";

    //接收消息
    public static void main(String[] args) throws IOException, TimeoutException {
        //创建工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setUsername("spring");
        connectionFactory.setPassword("spring");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        //声明    消费者成功消费的回调
        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println(new String(message.getBody()));
        };
        //消费者取消消费的回调
        CancelCallback cancelCallback = consumerTag -> {
            System.out.println("消息消费被中断");
        };
        //接收消息
        //消费的队列 是否自动应答  未成功的回调  消费者成功消费的回调
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, cancelCallback);
    }
}

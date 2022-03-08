package com.atguigu.rabbitmq.five;

import com.atguigu.rabbitmq.utils.RabbitMQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogs02 {

    //交换机名称
    public static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws Exception{
        Channel channel = RabbitMQUtils.getChannel();

        //声明一个队列,名称随机，当消费者断开与队列的连接时，队列自动删除
        String queueName = channel.queueDeclare().getQueue();

        //绑定交换机与队列
        channel.queueBind(queueName,EXCHANGE_NAME,"");
        System.out.println("等待接受消息，把接受到的消息打印在屏幕上...");


        DeliverCallback deliverCallback = (consumerTag,message) -> {
            System.out.println("ReceiveLogs02控制台打印接受到的消息：" + new String(message.getBody()));
        };

        channel.basicConsume(queueName,true,deliverCallback,consumerTag -> {});
    }

}

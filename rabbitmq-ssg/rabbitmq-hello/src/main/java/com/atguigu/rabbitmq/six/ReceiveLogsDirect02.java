package com.atguigu.rabbitmq.six;

import com.atguigu.rabbitmq.utils.RabbitMQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsDirect02 {
    public static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMQUtils.getChannel();

        //声明一个队列
        channel.queueDeclare("disk",false,false,false,null);

        //绑定交换机与队列
        channel.queueBind("disk",EXCHANGE_NAME,"error");


        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println("ReceiveLogsDirect02控制台打印接受到的消息：" + new String(message.getBody()));
        };

        channel.basicConsume("disk",true,deliverCallback,consumerTag -> {});
    }
}


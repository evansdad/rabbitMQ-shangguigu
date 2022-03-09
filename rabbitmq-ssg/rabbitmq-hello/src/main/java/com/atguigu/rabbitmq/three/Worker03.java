package com.atguigu.rabbitmq.three;

import com.atguigu.rabbitmq.utils.RabbitMQUtils;
import com.atguigu.rabbitmq.utils.SleepUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;

public class Worker03 {
    // 队列名称
    public static final String TASK_QUEUE_NAME = "ack_queue";

    public static void main(String[] args) throws Exception {
        Channel channel = RabbitMQUtils.getChannel();
        System.out.println("C1等待接受消息处理时间较短");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            // 等待一秒
            SleepUtils.sleep(1);
            System.out.println("接受到的消息是:"+new String(message.getBody()));

            //进行手动应答
            //消息的标记  tag
            //是否批量应答，false：不批量应答 true：批量
            channel.basicAck(message.getEnvelope().getDeliveryTag(),false);
        };

        //设置不公平分发
        int prefetchCount = 2;
        channel.basicQos(prefetchCount);
        // 采用手动应答
        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck,deliverCallback,(consumerTag) -> {
            System.out.println(consumerTag+"消费者取消消费接口回调逻辑");
        });
    }
}

package com.atguigu.rabbitmq.four;

import com.atguigu.rabbitmq.utils.RabbitMQUtils;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

//发布确认
//单个确认
//批量确认
//异步批量确认
public class ConfirmMessage {
    public static final int MESSAGE_COUNT = 1000;

    public static void main(String[] args) throws Exception {
        //单个确认
 //       ConfirmMessage.publishMessageIndividually();  //发布1000个单独确认消息,耗时1315ms
        //批量确认
//      ConfirmMessage.publishMessageBatch();   //发布1000个批量确认消息，耗时98ms
        //异步批量确认
       ConfirmMessage.publicMessageAsync();   //发布1000个异步确认消息，耗时91ms
    }

    //单个确认
    public static void publishMessageIndividually() throws IOException, TimeoutException, InterruptedException {
        Channel channel = RabbitMQUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, false, false, false, null);
        //开启发布确认
        channel.confirmSelect();
        long begin = System.currentTimeMillis();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            //服务端返回false或超时时间内未返回，生产者可以消息重发
            boolean flag = channel.waitForConfirms();
            if (flag){
                System.out.println("消息发送成功");
            }

        }
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个单独确认消息,耗时" + (end - begin) + "ms");
    }

    public static void publishMessageBatch() throws Exception{
        Channel channel = RabbitMQUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName,false,false,false,null);

        // 开启发布确认
        channel.confirmSelect();
        // 开始时间
        long begin = System.currentTimeMillis();

        // 批量确认消息大小
        int batchSize = 100;

        // 批量发送 批量确认
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = i + "";
            channel.basicPublish("",queueName,null,message.getBytes(StandardCharsets.UTF_8));
            // 判断达到100条消息的时候，批量确认一次
            if (i%batchSize == 0){
                // 确认发布
                channel.waitForConfirms();
            }
        }

        // 结束时间
        long end = System.currentTimeMillis();
        System.out.println("发布"+MESSAGE_COUNT+"个批量确认消息，耗时"+ (end - begin) + "ms");
    }

    public static void publicMessageAsync() throws Exception {
        Channel channel = RabbitMQUtils.getChannel();
        String queueName = UUID.randomUUID().toString();
        channel.queueDeclare(queueName, false, false, false, null);

        // 开启发布确认
        channel.confirmSelect();

        /**
         * * 线程安全有序的一个哈希表，适用于高并发的情况
         * * 1.轻松的将序号与消息进行关联
         * * 2.轻松批量删除条目 只要给到序列号
         * * 3.支持并发访问
         * */
        ConcurrentSkipListMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();

        // 开始时间
        long begin = System.currentTimeMillis();

        /**
         * * 确认收到消息的一个回调
         * * 1.消息序列号
         * * 2.true可以确认小于等于当前序列号的消息
         * * false确认当前序列号消息
         * */
        ConfirmCallback ackCallback = (deliveryTag, multiply) -> {
            if (multiply) {
                //返回的是小于等于当前序列号的未确认消息 是一个map
                ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(deliveryTag);
                //清除该部分未确认消息
                confirmed.clear();
            } else {
                //只清除当前序列号的消息
                outstandingConfirms.remove(deliveryTag);
            }

            System.out.println("确认的消息：" + deliveryTag);
        };

        // 消息确认失败回调函数
        /**
         * 参数1：消息的标记
         * 参数2：是否为批量确认
         * */
        ConfirmCallback nackCallback = (deliveryTag, multiply) -> {
            //打印未确认的消息
            System.out.println("未确认的消息：" + deliveryTag);
        };

        // 准备消息的监听器，监听哪些消息成功，哪些消息失败
        /*
         * 参数1：监听哪些消息成功
         * 参数2：监听哪些消息失败
         * */
        channel.addConfirmListener(ackCallback, nackCallback);

        // 批量发送消息
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = "消息" + i;
            channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
            //记录消息总和
            outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
        }

        // 结束时间
        long end = System.currentTimeMillis();
        System.out.println("发布" + MESSAGE_COUNT + "个异步确认消息，耗时" + (end - begin) + "ms");
    }
}

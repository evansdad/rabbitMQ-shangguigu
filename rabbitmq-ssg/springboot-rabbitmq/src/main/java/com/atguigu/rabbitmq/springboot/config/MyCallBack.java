package com.atguigu.rabbitmq.springboot.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
@Slf4j
public class MyCallBack implements RabbitTemplate.ConfirmCallback {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @PostConstruct
    public void init(){
        //注入
        rabbitTemplate.setConfirmCallback(this);
    }

    /*
     * 交换机确认回调方法,发消息后，交换机接收到了就回调
     *   1.1 correlationData：保存回调消息的ID及相关信息
     *   1.2 b:交换机收到消息，为true
     *   1.3 s:失败原因，成功为null
     *
     * 发消息，交换机接受失败，也回调
     *   2.1 correlationData：保存回调消息的ID及相关信息
     *   2.2 b:交换机没收到消息，为false
     *   2.3 s:失败的原因
     *
     * */

    @Override
    public void confirm(CorrelationData correlationData, boolean b, String s) {
        String id = correlationData!=null ? correlationData.getId():"";
        if (b){
            log.info("交换机已经收到ID为：{}的信息",id);
        }else {
            log.info("交换机还未收到ID为：{}的消息，由于原因：{}",id,s);
        }
    }
}

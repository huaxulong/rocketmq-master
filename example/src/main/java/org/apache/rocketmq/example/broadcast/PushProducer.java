package org.apache.rocketmq.example.broadcast;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @description: PushProducer
 * <p></p>
 * @author: DongxuHua
 * @create: at 2021-12-10 5:34 下午
 * @version: 1.0.0
 * @history: modify history             <desc>
 */
public class PushProducer {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name_broad");
        producer.setNamesrvAddr("127.0.0.1:9876");

        /*
         * Launch the instance.
         */
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};

        for (int i = 0; i < 5; i++) {
            Message msg = new Message("TopicBroadcastTest",
                    tags[i % tags.length],
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }

        producer.shutdown();
    }

}

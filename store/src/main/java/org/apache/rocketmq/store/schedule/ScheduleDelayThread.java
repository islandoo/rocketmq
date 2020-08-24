/**
 * Alipay.com Inc. Copyright (c) 2004-2020 All Rights Reserved.
 */
package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.DelayQueue;

/**
 *
 * @author tom
 * @version $Id: ScheduleDelayThread.java, v 0.1 2020-08-06 19:46 tom Exp $
 */
public class ScheduleDelayThread extends ServiceThread {
    private static final InternalLogger         log               = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private              DelayQueue<MessageExt> delayQueue        = new DelayQueue();
    private              String                 messageDelayLevel = "5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h 7205s";

    public static List<Long>   timeList;
    private       MessageStore messageStore;
    @Override
    public String getServiceName() {
        return "ScheduleDelayThread";
    }

    public ScheduleDelayThread(MessageStore messageStore) {
        this.messageStore = messageStore;
        timeList = new ArrayList<>();
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        String[] levelArray = messageDelayLevel.split(" ");
        for (int i = 0; i < levelArray.length; i++) {
            String value = levelArray[i];
            String ch = value.substring(value.length() - 1);
            Long tu = timeUnitTable.get(ch);

            long num = Long.parseLong(value.substring(0, value.length() - 1));
            long delayTimeMillis = tu * num;
            this.timeList.add(delayTimeMillis);
        }

    }

    public void put(MessageExt message) {
        delayQueue.put(message);
    }

    @Override
    public void run() {
        while (true) {
            MessageExt msgExt = null;
            try {
                msgExt = delayQueue.take();
                log.info("msgExt take time = " + (System.currentTimeMillis() - msgExt.getStoreTimestamp()));
                MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                if (TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC.equals(msgInner.getTopic())) {
                    log.error("[BUG] the real topic of schedule msg is {}, discard the msg. msg={}",
                            msgInner.getTopic(), msgInner);
                    continue;
                }
                PutMessageResult putMessageResult = messageStore.putMessage(msgInner);
                System.out.println(msgExt.getMsgId() + "_" + putMessageResult.getAppendMessageResult().getMsgId());
                if (putMessageResult != null
                        && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    continue;
                } else {
                    // XXX: warn and notify me
                    log.error(
                            "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                            msgExt.getTopic(), msgExt.getMsgId());
                    //ScheduleMessageService.this.updateOffset(this.delayLevel,
                    //        nextOffset);
                    //TODO
                    return;
                }
            } catch (Exception e) {
                log.error("ScheduleMessageService, messageTimeup execute error, drop it. msgExt=" + msgExt, e);
            }
        }
    }

    private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());

        TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
        long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
        msgInner.setTagsCode(tagsCodeValue);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

        msgInner.setWaitStoreMsgOK(false);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

        msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

        String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        int queueId = Integer.parseInt(queueIdStr);
        msgInner.setQueueId(queueId);
        msgInner.getProperties().put(MessageConst.PROPERTY_ORIGIN_MSG_ID, msgExt.getMsgId());
        msgInner.setKeys(msgExt.getKeys() + " " + msgExt.getMsgId());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        msgInner.setDelayTime(0L);
        return msgInner;
    }

    public static int getDelayTimeLevel(long delayTime) {
        for (int i = 0; i < timeList.size(); i++) {
            if (delayTime + 3000 < timeList.get(i)) {
                return i;
            }
        }
        return 0;
    }

}
/*
 *  Copyright (c) 2002-2023, Manorrock.com. All Rights Reserved.
 *
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *      1. Redistributions of source code must retain the above copyright
 *         notice, this list of conditions and the following disclaimer.
 *
 *      2. Redistributions in binary form must reproduce the above copyright
 *         notice, this list of conditions and the following disclaimer in the
 *         documentation and/or other materials provided with the distribution.
 *
 *      3. Neither the name of the copyright holder nor the names of its 
 *         contributors may be used to endorse or promote products derived from
 *         this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 */
package com.manorrock.colibri.kafka;

import com.manorrock.colibri.api.EventReceiver;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

/**
 * The Kafka implementation of an EventReceiver.
 *
 * @author Manfred Riem (mriem@manorrock.com)
 * @param <T> the type.
 */
public class KafkaEventReceiver<T> implements EventReceiver<T, String> {

    /**
     * Stores the consumer.
     */
    private KafkaConsumer<String, String> consumer;
    
    /**
     * Stores the pending records.
     */
    private ConsumerRecords<String, String> pendingRecords;
    
    /**
     * Stores the pending iterator.
     */
    private Iterator<ConsumerRecord<String, String>> pendingIterator;
    
    /**
     * Stores the topic.
     */
    private String topic;

    /**
     * Constructor.
     * 
     * @param properties the configuration properties.
     * @param topic the topic name.
     */
    public KafkaEventReceiver(Properties properties, String topic) {
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        this.topic = topic;
    }
    
    @Override
    public void close() {
        consumer.close();
    }

    @Override
    public T receive() {
        T result = null;
        try {
            if (pendingRecords == null) {
                pendingRecords = consumer.poll(Duration.ofSeconds(60));
                pendingIterator = pendingRecords.iterator();
            }
            if (pendingIterator.hasNext()) {
                result = toEvent(pendingIterator.next().value());
            } else {
                pendingRecords = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}

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
package com.manorrock.colibri.jms;

import com.manorrock.colibri.api.EventReceiver;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Queue;
import jakarta.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;

/**
 * The JMS TextMessage implementation of an EventReceiver.
 *
 * @author Manfred Riem (mriem@manorrock.com)
 * @param <T> the type.
 */
public class JmsTextMessageEventReceiver<T> implements EventReceiver<T, String> {

    /**
     * Stores the JMS context.
     */
    private final JMSContext context;
    
    /**
     * Stores the JMS producer.
     */
    private final JMSConsumer consumer;

    /**
     * Stores the JMS queue.
     */
    private final Queue queue;

    /**
     * Constructor.
     *
     * @param connectionFactory the connection factory.
     * @param destinationName the destination name.
     */
    public JmsTextMessageEventReceiver(
            ConnectionFactory connectionFactory,
            String destinationName) {
        context = connectionFactory.createContext();
        queue = context.createQueue(destinationName);
        consumer = context.createConsumer(queue);
    }
    
    @Override
    public void close() {
        consumer.close();
        context.close();
    }

    @Override
    public Map<String, Object> getDelegate() {
        Map<String, Object> delegate = new HashMap<>();
        delegate.put("jmsContext", context);
        delegate.put("jmsConsumer", consumer);
        delegate.put("queue", queue);
        return delegate;
    }

    @Override
    public T receive() {
        T result = null;
        try {
            TextMessage message = (TextMessage) consumer.receive();
            result = toEvent(message.getText());
        } catch (JMSException je) {
            je.printStackTrace();
        }
        return result;
    }
}

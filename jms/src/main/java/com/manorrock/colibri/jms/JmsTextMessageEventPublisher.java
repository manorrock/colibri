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

import com.manorrock.colibri.api.EventPublisher;
import jakarta.jms.ConnectionFactory;
import static jakarta.jms.DeliveryMode.NON_PERSISTENT;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Queue;

/**
 * The JMS TextMessage implementation of an EventPublisher.
 * 
 * @author Manfred Riem (mriem@manorrock.com)
 * @param <T> the type.
 */
public class JmsTextMessageEventPublisher<T> implements EventPublisher<T, String> {

    /**
     * Stores the context.
     */
    private final JMSContext context;
    
    /**
     * Stores the JMS producer.
     */
    private final JMSProducer producer;
    
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
    public JmsTextMessageEventPublisher(
            ConnectionFactory connectionFactory, String destinationName) {
        context = connectionFactory.createContext();
        queue = context.createQueue(destinationName);
        producer = context.createProducer();
        producer.setDeliveryMode(NON_PERSISTENT);
    }
    
    @Override
    public void close() throws Exception {
        context.close();
    }

    @Override
    public void publish(T event) {
        producer.send(queue, (String) toUnderlyingEvent(event));
    }

    @Override
    public String toUnderlyingEvent(T event) {
        return event.toString();
    }
}

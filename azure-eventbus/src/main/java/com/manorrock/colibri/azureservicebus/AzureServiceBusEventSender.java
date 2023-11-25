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
package com.manorrock.colibri.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.manorrock.colibri.api.EventSender;
import java.util.HashMap;
import java.util.Map;

/**
 * The Azure Event Bus implementation of an EventSender.
 *
 * @author Manfred Riem (mriem@manorrock.com)
 * @param <T> the type.
 */
public class AzureServiceBusEventSender<T> implements EventSender<T, byte[]> {

    /**
     * Stores the client.
     */
    private ServiceBusSenderClient client;
    
    /**
     * Stores the connection string.
     */
    private String connectionString;
    
    /**
     * Stores the queue name.
     */
    private String queueName;
    
    /**
     * Constructor.
     * 
     * @param connectionString the connection string.
     * @param queueName the queue name.
     */
    public AzureServiceBusEventSender(String connectionString, String queueName) {
        client = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .sender()
                .queueName(queueName)
                .buildClient();
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public Map<String, Object> getDelegate() {
        Map<String, Object> delegate = new HashMap<>();
        delegate.put("client", client);
        delegate.put("connectionString", connectionString);
        delegate.put("queueName", queueName);
        return delegate;
    }

    @Override
    public void send(T event) {
        ServiceBusMessage message = new ServiceBusMessage(toUnderlyingEvent(event));
        client.sendMessage(message);
    }
}

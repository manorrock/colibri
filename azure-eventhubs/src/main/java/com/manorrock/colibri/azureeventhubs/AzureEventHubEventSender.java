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
package com.manorrock.colibri.azureeventhubs;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.manorrock.colibri.api.EventSender;
import java.util.HashMap;
import java.util.Map;

/**
 * The Azure Event Bus implementation of an EventSender.
 *
 * @author Manfred Riem (mriem@manorrock.com)
 * @param <T> the type.
 */
public class AzureEventHubEventSender<T> implements EventSender<T, EventData> {

    /**
     * Stores the client.
     */
    private EventHubProducerClient client;
    
    /**
     * Stores the connection string.
     */
    private String connectionString;
    
    /**
     * Stores the event hub name.
     */
    private String eventHubName;
    
    /**
     * Constructor.
     * 
     * @param connectionString the connection string.
     * @param eventHubName the event hub name.
     */
    public AzureEventHubEventSender(String connectionString, String eventHubName) {
        this.connectionString = connectionString;
        this.eventHubName = eventHubName;
        client = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .buildProducerClient();
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
        delegate.put("eventHubName", eventHubName);
        return delegate;
    }

    @Override
    public void send(T event) {
        EventDataBatch batch = client.createBatch();
        batch.tryAdd(toUnderlyingEvent(event));
        client.send(batch);
    }
}

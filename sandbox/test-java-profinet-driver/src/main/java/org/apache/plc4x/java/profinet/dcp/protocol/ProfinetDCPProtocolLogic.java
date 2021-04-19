/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
package org.apache.plc4x.java.profinet.dcp.protocol;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import ch.qos.logback.classic.util.LogbackMDCAdapter;
import org.apache.plc4x.java.api.exceptions.PlcException;
import org.apache.plc4x.java.api.messages.*;
import org.apache.plc4x.java.api.model.PlcConsumerRegistration;
import org.apache.plc4x.java.api.model.PlcField;
import org.apache.plc4x.java.api.model.PlcSubscriptionHandle;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.api.types.PlcSubscriptionType;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.profinet.dcp.configuration.ProfinetConfiguration;
import org.apache.plc4x.java.profinet.dcp.field.IdentifyRequestField;
import org.apache.plc4x.java.profinet.dcp.field.ProfinetDcpField;
import org.apache.plc4x.java.profinet.dcp.protocol.conversation.IdentifyRequestConversation;
import org.apache.plc4x.java.profinet.dcp.readwrite.*;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.*;
import org.apache.plc4x.java.spi.ConversationContext;
import org.apache.plc4x.java.spi.Plc4xProtocolBase;
import org.apache.plc4x.java.spi.configuration.HasConfiguration;
import org.apache.plc4x.java.spi.context.DriverContext;
import org.apache.plc4x.java.spi.generation.ParseException;
import org.apache.plc4x.java.spi.generation.ReadBuffer;
import org.apache.plc4x.java.spi.messages.*;
import org.apache.plc4x.java.spi.messages.utils.ResponseItem;
import org.apache.plc4x.java.spi.model.DefaultPlcConsumerRegistration;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionField;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionHandle;
import org.apache.plc4x.java.spi.transaction.RequestTransactionManager;
import org.apache.plc4x.java.spi.values.PlcNull;
import org.apache.plc4x.java.spi.values.PlcValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Driver logic for handling Profinet-DCP packets.
 */
public class ProfinetDCPProtocolLogic extends Plc4xProtocolBase<BaseEthernetFrame> implements
    HasConfiguration<ProfinetConfiguration>, PlcSubscriber {

    public static MacAddress PROFINET_BROADCAST = createAddress(0x01, 0x0E, 0xCF, 0x00, 0x00, 0x00);
    public static short VLAN = (short) 0x8100;
    public static short PN_DCP = (short) 0x8892;

    public static final Duration REQUEST_TIMEOUT = Duration.ofMillis(10000);
    private final Logger logger = LoggerFactory.getLogger(ProfinetDCPProtocolLogic.class);
    private Map<DefaultPlcConsumerRegistration, Consumer<PlcSubscriptionEvent>> consumers = new ConcurrentHashMap<>();
    private RequestTransactionManager tm;

    private AtomicLong invokeId = new AtomicLong(0);
    private ProfinetConfiguration configuration;

    @Override
    public void setConfiguration(ProfinetConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void setDriverContext(DriverContext driverContext) {
        super.setDriverContext(driverContext);

        this.tm = new RequestTransactionManager();
    }

    @Override
    public void onConnect(ConversationContext<BaseEthernetFrame> context) {
        context.fireConnected();
    }

    @Override
    public CompletableFuture<PlcWriteResponse> write(PlcWriteRequest writeRequest) {
        CompletableFuture<PlcWriteResponse> response = new CompletableFuture<>();
        if (writeRequest.getFieldNames().size() != 1) {
            response.completeExceptionally(new IllegalArgumentException("You can write only one field at the time"));
            return response;
        }

        PlcField field = writeRequest.getFields().get(0);
        if (!(field instanceof ProfinetDcpField)) {
            response.completeExceptionally(new IllegalArgumentException("Only ProfinetDcpField instances are supported"));
            return response;
        }

        if (field instanceof IdentifyRequestField) {
            writeInternally((DefaultPlcWriteRequest) writeRequest, (IdentifyRequestField) field, response);
            return response;
        }

        response.completeExceptionally(new IllegalArgumentException("Only IdentifyRequestField instances are supported"));
        return response;
    }

    private void writeInternally(DefaultPlcWriteRequest writeRequest, IdentifyRequestField field, CompletableFuture<PlcWriteResponse> response) {
        final RequestTransactionManager.RequestTransaction transaction = tm.startRequest();

        String fieldName = writeRequest.getFieldNames().iterator().next();

        CompletableFuture<PlcResponseCode> callback = new CompletableFuture<>();
        callback.whenComplete((code, error) -> {
            if (error != null) {
                if (error instanceof PlcException) {
                    response.complete(new DefaultPlcWriteResponse(writeRequest, Collections.singletonMap(fieldName, PlcResponseCode.REMOTE_ERROR)));
                } else {
                    response.complete(new DefaultPlcWriteResponse(writeRequest, Collections.singletonMap(fieldName, PlcResponseCode.INTERNAL_ERROR)));
                }
                transaction.endRequest();
                return;
            }
            response.complete(new DefaultPlcWriteResponse(writeRequest, Collections.singletonMap(fieldName, code)));
            transaction.endRequest();
        });

        IdentifyRequestConversation conversation = new IdentifyRequestConversation(this.context, configuration.getSender(), invokeId::incrementAndGet, field);
        transaction.submit(() -> conversation.execute(callback));
    }

    @Override
    public void onDisconnect(ConversationContext<BaseEthernetFrame> context) {
        context.fireDisconnected();
    }

    @Override
    protected void decode(ConversationContext<BaseEthernetFrame> context, BaseEthernetFrame msg) throws Exception {
        if (msg instanceof TaggedFrame) {
            TaggedFrame frame = (TaggedFrame) msg;
            if ((short) frame.getEthernetType() != PN_DCP) {
                logger.trace("Discarding unwanted tagged frame with type 0x{}", Integer.toHexString(frame.getEthernetType()));
                return;
            }
        } else if ((short) msg.getEtherType() != PN_DCP) {
            logger.trace("Discarding unwanted ethernet frame type 0x{}", Integer.toHexString(msg.getEtherType()));
            return;
        }

        ProfinetFrame profinetFrame = msg.getPayload();

        for (Map.Entry<DefaultPlcConsumerRegistration, Consumer<PlcSubscriptionEvent>> entry : consumers.entrySet()) {
            DefaultPlcConsumerRegistration registration = entry.getKey();
            Consumer<PlcSubscriptionEvent> consumer = entry.getValue();

            for (PlcSubscriptionHandle handler : registration.getSubscriptionHandles()) {
                ProfinetDCPSubscriptionHandle handle = (ProfinetDCPSubscriptionHandle) handler;

                if (handle.matches(profinetFrame)) {
                    logger.trace("Dispatching frame {} to {}", profinetFrame, handle);
                    consumer.accept(handle.transform(profinetFrame));
                }
            }
        }

        /*
        if (profinetFrame.getFrameType() == FrameType.IDENTIFY_RESPONSE) {
            logger.info("Ident response from Profinet device:");
            if (profinetFrame.getFrame() instanceof DcpIdentResponsePDU) {
                DcpIdentResponsePDU response = (DcpIdentResponsePDU) profinetFrame.getFrame();
                DCPBlock[] blocks = response.getBlocks();
                for (int index = 0; index < blocks.length; index++) {
                    DCPBlock block = blocks[index];
                    if (block instanceof IP) {
                        IP ip = (IP) block;
                        logger.info("Device IP: {}, mask: {}, gateway: {}", addressString(ip.getIpAddress()), addressString(ip.getSubnetMask()), addressString(ip.getStandardGateway()));
                    }
                    if (block instanceof DeviceProperties) {
                        DeviceProperties properties = (DeviceProperties) block;
                        logger.info("Device option: {}, value: {}", properties.getSubOption().name(), properties.getProperties());
                    }
                }
            } else {
                logger.error("Unexpected ident response {}", profinetFrame.getFrame().getClass());
            }
        }
        //*/
    }

    @Override
    public CompletableFuture<PlcSubscriptionResponse> subscribe(PlcSubscriptionRequest request) {
        DefaultPlcSubscriptionRequest rq = (DefaultPlcSubscriptionRequest) request;

        Map<String, ResponseItem<PlcSubscriptionHandle>> answers = new LinkedHashMap<>();
        DefaultPlcSubscriptionResponse response = new DefaultPlcSubscriptionResponse(rq, answers);

        for (String key : rq.getFieldNames()) {
            DefaultPlcSubscriptionField subscription = (DefaultPlcSubscriptionField) rq.getField(key);
            if (subscription.getPlcSubscriptionType() != PlcSubscriptionType.EVENT) {
                answers.put(key, new ResponseItem<>(PlcResponseCode.UNSUPPORTED, null));
            } else if ((subscription.getPlcField() instanceof ProfinetDcpField)) {
                answers.put(key, new ResponseItem<>(PlcResponseCode.OK,
                    new ProfinetDCPSubscriptionHandle(this, key, (ProfinetDcpField) subscription.getPlcField())
                ));
            } else {
                answers.put(key, new ResponseItem<>(PlcResponseCode.INVALID_ADDRESS, null));
            }
        }

        return CompletableFuture.completedFuture(response);
    }

    @Override
    public PlcConsumerRegistration register(Consumer<PlcSubscriptionEvent> consumer, Collection<PlcSubscriptionHandle> handles) {
        final DefaultPlcConsumerRegistration consumerRegistration = new DefaultPlcConsumerRegistration(this, consumer, handles.toArray(new DefaultPlcSubscriptionHandle[0]));
        consumers.put(consumerRegistration, consumer);
        return consumerRegistration;
    }

    @Override
    public void unregister(PlcConsumerRegistration registration) {
        consumers.remove(registration);
    }


    private String addressString(IPv4Address address) {
        return address.getOctet1() + "." + address.getOctet2() + "." + address.getOctet3() + "." + address.getOctet4();
    }

    @Override
    public void close(ConversationContext<BaseEthernetFrame> context) {

    }

    public final static MacAddress createAddress(int octet1, int octet2, int octet3, int octet4, int octet5, int octet6) {
        return new MacAddress((short) octet1, (short) octet2, (short) octet3, (short) octet4, (short) octet5, (short) octet6);
    }

}

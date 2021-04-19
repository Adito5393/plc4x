/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.plc4x.java.profinet.dcp.protocol.conversation;

import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.profinet.dcp.field.IdentifyRequestField;
import org.apache.plc4x.java.profinet.dcp.protocol.ProfinetDCPProtocolLogic;
import org.apache.plc4x.java.profinet.dcp.readwrite.AllSelector;
import org.apache.plc4x.java.profinet.dcp.readwrite.BaseEthernetFrame;
import org.apache.plc4x.java.profinet.dcp.readwrite.DCPBlock;
import org.apache.plc4x.java.profinet.dcp.readwrite.DcpIdentRequestPDU;
import org.apache.plc4x.java.profinet.dcp.readwrite.EthernetFrame;
import org.apache.plc4x.java.profinet.dcp.readwrite.MacAddress;
import org.apache.plc4x.java.profinet.dcp.readwrite.ProfinetFrame;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.DCPServiceID;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.DCPServiceType;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.FrameType;
import org.apache.plc4x.java.spi.ConversationContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Implementation of message exchange needed to cover identify request.
 *
 * Since this is broadcast operation overall implementation is quite simple.
 */
public class IdentifyRequestConversation implements Conversation<PlcResponseCode> {
    private final ConversationContext<BaseEthernetFrame> context;
    private final MacAddress sender;
    private final Supplier<Long> xid;
    private final IdentifyRequestField identifyRequest;

    public IdentifyRequestConversation(ConversationContext<BaseEthernetFrame> context, MacAddress sender, Supplier<Long> xid, IdentifyRequestField identifyRequest) {
        this.context = context;
        this.sender = sender;
        this.xid = xid;
        this.identifyRequest = identifyRequest;
    }

    public void execute(CompletableFuture<PlcResponseCode> callback) {
        DCPServiceID serviceId = DCPServiceID.IDENTIFY;
        DCPServiceType serviceType = DCPServiceType.REQUEST;

        DCPBlock[] blocks = identifyRequest.getBlocks().stream().map(AllSelector::new).toArray(DCPBlock[]::new);

        DcpIdentRequestPDU requestPDU = new DcpIdentRequestPDU(serviceId, serviceType, xid.get(), identifyRequest.getResponseDelay(), blocks);
        ProfinetFrame dcpFrame = new ProfinetFrame(FrameType.IDENTIFY_MULTICAST_REQUEST, requestPDU);
        EthernetFrame ethernetFrame = new EthernetFrame(ProfinetDCPProtocolLogic.PROFINET_BROADCAST, sender, ProfinetDCPProtocolLogic.PN_DCP, dcpFrame);

        try {
            context.sendToWire(ethernetFrame);
            callback.complete(PlcResponseCode.OK);
        } catch (Exception e) {
            callback.completeExceptionally(e);
        }
    }
}

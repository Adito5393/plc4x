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
package org.apache.plc4x.java.profinet.dcp.protocol;

import org.apache.plc4x.java.api.messages.PlcSubscriptionEvent;
import org.apache.plc4x.java.api.types.PlcResponseCode;
import org.apache.plc4x.java.api.value.PlcValue;
import org.apache.plc4x.java.profinet.dcp.field.IdentifyResponseField;
import org.apache.plc4x.java.profinet.dcp.field.ProfinetDcpField;
import org.apache.plc4x.java.profinet.dcp.readwrite.*;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.DCPBlockOption;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.FrameType;
import org.apache.plc4x.java.spi.messages.DefaultPlcSubscriptionEvent;
import org.apache.plc4x.java.spi.messages.PlcSubscriber;
import org.apache.plc4x.java.spi.messages.utils.ResponseItem;
import org.apache.plc4x.java.spi.model.DefaultPlcSubscriptionHandle;
import org.apache.plc4x.java.spi.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Handle responsible for deciding if received message exchange is matching and transforming DCP PDU.
 *
 * Given that Profinet DCP frames are quite heavy and hard to navigate they are wrapped into {@link PlcStruct} which is
 * based on simple map.
 */
public class ProfinetDCPSubscriptionHandle extends DefaultPlcSubscriptionHandle {

    private final Logger logger = LoggerFactory.getLogger(ProfinetDCPSubscriptionHandle.class);
    private final String name;
    private final ProfinetDcpField field;

    public ProfinetDCPSubscriptionHandle(PlcSubscriber subscriber, String name, ProfinetDcpField field) {
        super(subscriber);
        this.name = name;
        this.field = field;
    }

    public String getName() {
        return name;
    }

    boolean matches(ProfinetFrame pdu) {
        if (pdu.getFrameType() == FrameType.IDENTIFY_RESPONSE && field instanceof IdentifyResponseField) {
            List<DCPBlockOption> fieldBlocks = ((IdentifyResponseField) field).getBlocks();
            if (fieldBlocks.contains(DCPBlockOption.ALL_SELECTOR)) {
                return true;
            }

            if (pdu.getFrame() instanceof DcpIdentResponsePDU) {
                DCPBlock[] blocks = ((DcpIdentResponsePDU) pdu.getFrame()).getBlocks();
                for (DCPBlock block : blocks) {
                    if (fieldBlocks.contains(block.getBlockType())) {
                        return true;
                    }
                }
            }
        }

        logger.trace("Unmatched profinet identify response {}. Ignoring", pdu.getFrame());
        // TODO implement rest of matching logic
        return false;
    }

    public ProfinetDcpField getField() {
        return field;
    }

    public String toString() {
        return "ProfinetDCPSubscriptionHandle [service=" + field + "]";
    }

    public PlcSubscriptionEvent transform(ProfinetFrame pdu) {
        if (pdu.getFrameType() == FrameType.IDENTIFY_RESPONSE) {
            ProfinetData frame = pdu.getFrame();
            if (frame instanceof DcpIdentResponsePDU) {
                DCPBlock[] blocks = ((DcpIdentResponsePDU) frame).getBlocks();
                Map<String, PlcValue> struct = new LinkedHashMap<>();
                for (DCPBlock block : blocks) {
                    if (block instanceof IP) {
                        IP ip = (IP) block;
                        struct.put("ip.address", PlcSTRING.of(addressToString(ip.getIpAddress(), ".")));
                        struct.put("ip.mask", PlcSTRING.of(addressToString(ip.getSubnetMask(), ".")));
                        struct.put("ip.gateway", PlcSTRING.of(addressToString(ip.getStandardGateway(), ".")));

                    }

                    if (block instanceof DeviceProperties) {
                        DeviceProperties wrapper = (DeviceProperties) block;
                        DCPDeviceProperties property = wrapper.getProperties();
                        //System.out.println("Device property " + property + " " + property.getClass());

                        if (property instanceof DeviceOptions) {
                            // todo map it somehow
                        } else if (property instanceof StationType) {
                            struct.put("station.type", PlcSTRING.of(((StationType) property).getVendorNameForDevice().getText()));
                        } else if (property instanceof StationName) {
                            struct.put("station.name", PlcSTRING.of(((StationName) property).getName().getText()));
                        } else if (property instanceof DeviceId) {
                            struct.put("device.id", PlcINT.of(((DeviceId) property).getDeviceId()));
                        } else if (property instanceof DeviceRole) {
                            struct.put("device.role", PlcBYTE.of(((DeviceRole) property).getRole()));
                        } else if (property instanceof DeviceInstance) {
                            struct.put("device.instance.low", PlcBYTE.of(((DeviceInstance) property).getInstanceLow()));
                            struct.put("device.instance.high", PlcBYTE.of(((DeviceInstance) property).getInstanceHigh()));
                        } else {
                            logger.trace("Unsupported device property {}", property);
                        }
                    }
                }

                return new DefaultPlcSubscriptionEvent(Instant.now(), Collections.singletonMap(
                    getName(), new ResponseItem<>(PlcResponseCode.OK, new PlcStruct(struct)))
                );
            }
        }

        throw new IllegalArgumentException("Frame " + pdu.getFrameType() + " not supported");
    }

    private String addressToString(IPv4Address address, String separator) {
        return address.getOctet1() + separator + address.getOctet2() + separator + address.getOctet3() + separator + address.getOctet4();
    }
}

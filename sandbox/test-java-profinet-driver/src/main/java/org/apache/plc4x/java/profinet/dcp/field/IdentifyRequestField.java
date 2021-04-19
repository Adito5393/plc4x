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
package org.apache.plc4x.java.profinet.dcp.field;

import org.apache.plc4x.java.api.exceptions.PlcInvalidFieldException;
import org.apache.plc4x.java.profinet.dcp.readwrite.types.DCPBlockOption;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Definition of identify field which can accept Profinet DCP block options.
 *
 * Supported syntax:
 * IDENTIFY_BROADCAST:
 * IDENTIFY_BROADCAST:/responseDelay
 * IDENTIFY_BROADCAST:BLOCK/responseDelay
 * IDENTIFY_BROADCAST:IP,DHCP/responseDelay
 *
 * Block options are defined as {@link IdentifyField#BLOCK_PATTERN}. If no block is defined then ALL is assumed.
 */
public class IdentifyRequestField extends BaseIdentifyField implements IdentifyField {

    public static final Pattern ADDRESS_PATTERN = Pattern.compile("IDENTIFY_BROADCAST:" + BLOCK_ADDRESS_PATTERN +"(?:\\/(?<responseDelay>\\d+))?");

    private final List<DCPBlockOption> blocks;
    private final int responseDelay;

    public IdentifyRequestField(List<DCPBlockOption> blocks) {
        this(blocks, 1000);
    }

    public IdentifyRequestField(List<DCPBlockOption> blocks, int responseDelay) {
        this.blocks = blocks;
        this.responseDelay = responseDelay;
    }

    public List<DCPBlockOption> getBlocks() {
        return blocks;
    }

    public int getResponseDelay() {
        return responseDelay;
    }

    public static boolean matches(String fieldQuery) {
        return ADDRESS_PATTERN.matcher(fieldQuery).matches();
    }

    public static Matcher getMatcher(String addressString) throws PlcInvalidFieldException {
        Matcher matcher = ADDRESS_PATTERN.matcher(addressString);
        if (matcher.matches()) {
            return matcher;
        }

        throw new PlcInvalidFieldException(addressString, ADDRESS_PATTERN);
    }

    public static IdentifyRequestField of(String addressString) {
        Matcher matcher = getMatcher(addressString);
        List<DCPBlockOption> blocks = parseBlocks(matcher);

        String responseDelayGroup = matcher.group("responseDelay");
        if (responseDelayGroup != null) {
            return new IdentifyRequestField(blocks, Integer.parseInt(responseDelayGroup));
        }

        return new IdentifyRequestField(blocks);
    }

}
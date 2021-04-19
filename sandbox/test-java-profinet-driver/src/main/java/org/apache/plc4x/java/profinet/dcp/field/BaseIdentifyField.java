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

import org.apache.plc4x.java.profinet.dcp.readwrite.types.DCPBlockOption;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Base type for identification fields which allows to parse blocks section field expression.
 */
abstract class BaseIdentifyField extends ProfinetDcpField implements IdentifyField {

    protected static List<DCPBlockOption> parseBlocks(Matcher matcher) {
        String requestedBlocks = matcher.group("block");

        List<DCPBlockOption> blocks = new ArrayList<>();
        if (requestedBlocks == null || requestedBlocks.trim().isEmpty()) {
            blocks.add(DCPBlockOption.ALL_SELECTOR);
        } else if (requestedBlocks.contains(",")) {
            Arrays.stream(requestedBlocks.split(","))
                .map(String::trim)
                .map(String::toUpperCase)
                .map(DCPBlockOption::valueOf)
                .forEach(blocks::add);
        } else {
            blocks.add(DCPBlockOption.valueOf(requestedBlocks.trim().toUpperCase()));
        }
        return blocks;
    }

}


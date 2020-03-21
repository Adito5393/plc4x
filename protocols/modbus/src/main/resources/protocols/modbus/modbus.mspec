//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

// Remark: The different fields are encoded in Big-endian.

[type 'ModbusConstants'
    [const          uint 16     'modbusTcpDefaultPort' '502']
]

[type 'ModbusTcpADU' [bit 'response']
    // It is used for transaction pairing, the MODBUS server copies in the response the transaction
    // identifier of the request.
    [simple         uint 16     'transactionIdentifier']

    // It is used for intra-system multiplexing. The MODBUS protocol is identified by the value 0.
    [const          uint 16     'protocolIdentifier'    '0x0000']

    // The length field is a byte count of the following fields, including the Unit Identifier and
    // data fields.
    [implicit       uint 16     'length'                'pdu.lengthInBytes + 1']

    // This field is used for intra-system routing purpose. It is typically used to communicate to
    // a MODBUS+ or a MODBUS serial line slave through a gateway between an Ethernet TCP-IP network
    // and a MODBUS serial line. This field is set by the MODBUS Client in the request and must be
    // returned with the same value in the response by the server.
    [simple         uint 8      'unitIdentifier']

    // The actual modbus payload
    [simple         ModbusPDU   'pdu' ['response']]
]

[type 'ModbusSerialADU' [bit 'response']
    [simple         uint 16     'transactionId']
    [reserved       uint 16     '0x0000']
    [simple         uint 16     'length']
    [simple         uint 8      'address']

    // The actual modbus payload
    [simple         ModbusPDU   'pdu' ['response']]
]

[discriminatedType 'ModbusPDU' [bit 'response']
    [implicit       bit         'error'     'DISCRIMINATOR_VALUES[0]']
    [implicit       uint 7      'function'  'DISCRIMINATOR_VALUES[1]']
    [typeSwitch 'error','function','response'
        ['true'                     ModbusPDUError
            [simple     uint 8      'exceptionCode']
        ]

        // Bit Access
        ['false','0x02','false'     ModbusPDUReadDiscreteInputsRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]
        ['false','0x02','true'      ModbusPDUReadDiscreteInputsResponse
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x01','false'     ModbusPDUReadCoilsRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]
        ['false','0x01','true'      ModbusPDUReadCoilsResponse
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x05','false'     ModbusPDUWriteSingleCoilRequest
            [simple     uint 16     'address']
            [simple     uint 16     'value']
        ]
        ['false','0x05','true'      ModbusPDUWriteSingleCoilResponse
            [simple     uint 16     'address']
            [simple     uint 16     'value']
        ]

        ['false','0x0F','false'     ModbusPDUWriteMultipleCoilsRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]
        ['false','0x0F','true'      ModbusPDUWriteMultipleCoilsResponse
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]

        // Uint 16 Access (short)
        ['false','0x04','false'     ModbusPDUReadInputRegistersRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]
        ['false','0x04','true'      ModbusPDUReadInputRegistersResponse
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x03','false'     ModbusPDUReadHoldingRegistersRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]
        ['false','0x03','true'      ModbusPDUReadHoldingRegistersResponse
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x06','false'     ModbusPDUWriteSingleRegisterRequest
            [simple     uint 16     'address']
            [simple     uint 16     'value']
        ]
        ['false','0x06','true'      ModbusPDUWriteSingleRegisterResponse
            [simple     uint 16     'address']
            [simple     uint 16     'value']
        ]

        ['false','0x10','false'     ModbusPDUWriteMultipleHoldingRegistersRequest
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]
        ['false','0x10','true'      ModbusPDUWriteMultipleHoldingRegistersResponse
            [simple     uint 16     'startingAddress']
            [simple     uint 16     'quantity']
        ]

        ['false','0x17','false'     ModbusPDUReadWriteMultipleHoldingRegistersRequest
            [simple     uint 16     'readStartingAddress']
            [simple     uint 16     'readQuantity']
            [simple     uint 16     'writeStartingAddress']
            [simple     uint 16     'writeQuantity']
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]
        ['false','0x17','true'      ModbusPDUReadWriteMultipleHoldingRegistersResponse
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x16','false'     ModbusPDUMaskWriteHoldingRegisterRequest
            [simple     uint 16     'referenceAddress']
            [simple     uint 16     'andMask']
            [simple     uint 16     'orMask']
        ]
        ['false','0x16','true'      ModbusPDUMaskWriteHoldingRegisterResponse
            [simple     uint 16     'referenceAddress']
            [simple     uint 16     'andMask']
            [simple     uint 16     'orMask']
        ]

        ['false','0x18','false'     ModbusPDUReadFifoQueueRequest
            [simple     uint 16     'fifoPointerAddress']
        ]
        ['false','0x18','true'      ModbusPDUReadFifoQueueResponse
            [implicit   uint 16     'byteCount'     '(COUNT(fifoValue) * 2) + 2']
            [implicit   uint 16     'fifoCount'     '(COUNT(fifoValue) * 2) / 2']
            [array      uint 16     'fifoValue'     count   'fifoCount']
        ]

        // File Record Access
        ['false','0x14','false'     ModbusPDUReadFileRecordRequest
            [implicit   uint 8      'byteCount'                 'ARRAY_SIZE_IN_BYTES(items)']
            [array      ModbusPDUReadFileRecordRequestItem      'items' length 'byteCount']
        ]
        ['false','0x14','true'      ModbusPDUReadFileRecordResponse
            [implicit   uint 8      'byteCount'                 'ARRAY_SIZE_IN_BYTES(items)']
            [array      ModbusPDUReadFileRecordResponseItem     'items' length 'byteCount']
        ]

        ['false','0x15','false'     ModbusPDUWriteFileRecordRequest
            [implicit   uint 8      'byteCount'                 'ARRAY_SIZE_IN_BYTES(items)']
            [array      ModbusPDUWriteFileRecordRequestItem     'items' length 'byteCount']
        ]
        ['false','0x15','true'      ModbusPDUWriteFileRecordResponse
            [implicit   uint 8      'byteCount'                 'ARRAY_SIZE_IN_BYTES(items)']
            [array      ModbusPDUWriteFileRecordResponseItem    'items' length 'byteCount']
        ]

        // Diagnostics (Serial Line Only)
        ['false','0x07','false'     ModbusPDUReadExceptionStatusRequest
        ]
        ['false','0x07','true'      ModbusPDUReadExceptionStatusResponse
            [simple     uint 8      'value']
        ]

        ['false','0x08','false'     ModbusPDUDiagnosticRequest
            // TODO: Implement the sub-request discriminated type [simple uint 8  'subfunction']
        ]
        ['false','0x08','true'      ModbusPDUDiagnosticResponse
            // TODO: Implement the sub-request discriminated type [simple uint 8  'subfunction']
        ]

        ['false','0x0B','false'     ModbusPDUGetComEventCounterRequest
        ]
        ['false','0x0B','true'      ModbusPDUGetComEventCounterResponse
            [simple     uint 16     'status']
            [simple     uint 16     'eventCount']
        ]

        ['false','0x0C','false'     ModbusPDUGetComEventLogRequest
        ]
        ['false','0x0C','true'      ModbusPDUGetComEventLogResponse
            [implicit   uint 8      'byteCount'    'COUNT(events) + 6']
            [simple     uint 16     'status']
            [simple     uint 16     'eventCount']
            [simple     uint 16     'messageCount']
            [array      int 8       'events'       count   'byteCount - 6']
        ]

        ['false','0x11','false'     ModbusPDUReportServerIdRequest
        ]
        ['false','0x11','true'      ModbusPDUReportServerIdResponse
            // TODO: This is not specified very well in the spec ... investigate.
            [implicit   uint 8      'byteCount'     'COUNT(value)']
            [array      int 8       'value'         count   'byteCount']
        ]

        ['false','0x2B','false'     ModbusPDUReadDeviceIdentificationRequest
        ]
        ['false','0x2B','true'      ModbusPDUReadDeviceIdentificationResponse
        ]
    ]
]

[type 'ModbusPDUReadFileRecordRequestItem'
    [simple     uint 8     'referenceType']
    [simple     uint 16    'fileNumber']
    [simple     uint 16    'recordNumber']
    [simple     uint 16    'recordLength']
]

[type 'ModbusPDUReadFileRecordResponseItem'
    [implicit   uint 8     'dataLength'     '(COUNT(data) * 2) + 1']
    [simple     uint 8     'referenceType']
    [array      uint 16    'data'           length  'dataLength - 1']
]

[type 'ModbusPDUWriteFileRecordRequestItem'
    [simple     uint 8     'referenceType']
    [simple     uint 16    'fileNumber']
    [simple     uint 16    'recordNumber']
    [implicit   uint 16    'recordLength'   '(COUNT(recordData) * 2) / 2']
    [array      uint 16    'recordData'     length  'recordLength * 2']
]

[type 'ModbusPDUWriteFileRecordResponseItem'
    [simple     uint 8     'referenceType']
    [simple     uint 16    'fileNumber']
    [simple     uint 16    'recordNumber']
    [implicit   uint 16    'recordLength'   '(COUNT(recordData) * 2) / 2']
    [array      uint 16    'recordData'     length  'recordLength * 2']
]

[dataIo 'DataItem' [uint 8 'dataType', uint 8 'numberOfValues']
    [typeSwitch 'dataType','numberOfValues'
        ['1','1' Boolean
            [reserved uint 7 '0x00']
            [simple   bit    'value']
        ]
        ['1' List
            [array bit 'value' count 'numberOfValues']
        ]
        ['2','1' Integer
            [simple int 16 'value']
        ]
        ['2' List
            [array int 16 'value' count 'numberOfValues']
        ]
    ]
]


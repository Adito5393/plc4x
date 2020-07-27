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
#ifndef PLC4C_S7_READ_WRITE_S7_PAYLOAD_H_
#define PLC4C_S7_READ_WRITE_S7_PAYLOAD_H_

#include <stdbool.h>
#include <stdint.h>
#include <plc4c/spi/read_buffer.h>
#include <plc4c/spi/write_buffer.h>
#include <plc4c/utils/list.h>
#include "s7_payload_user_data_item.h"
#include "s7_var_payload_status_item.h"
#include "s7_var_payload_data_item.h"
#include "s7_parameter.h"
#include "s7_payload.h"

#ifdef __cplusplus
extern "C" {
#endif


// Structure used to contain the discriminator values for discriminated types using this as a parent
struct plc4c_s7_read_write_s7_payload_discriminator {
  uint8_t messageType;
  uint8_t parameterParameterType;
};
typedef struct plc4c_s7_read_write_s7_payload_discriminator plc4c_s7_read_write_s7_payload_discriminator;

// Enum assigning each sub-type an individual id.
enum plc4c_s7_read_write_s7_payload_type {
  plc4c_s7_read_write_s7_payload_type_plc4c_s7_read_write_s7_payload_read_var_response = 0,
  plc4c_s7_read_write_s7_payload_type_plc4c_s7_read_write_s7_payload_write_var_request = 1,
  plc4c_s7_read_write_s7_payload_type_plc4c_s7_read_write_s7_payload_write_var_response = 2,
  plc4c_s7_read_write_s7_payload_type_plc4c_s7_read_write_s7_payload_user_data = 3};
typedef enum plc4c_s7_read_write_s7_payload_type plc4c_s7_read_write_s7_payload_type;

// Function to get the discriminator values for a given type.
plc4c_s7_read_write_s7_payload_discriminator plc4c_s7_read_write_s7_payload_get_discriminator(plc4c_s7_read_write_s7_payload_type type);

struct plc4c_s7_read_write_s7_payload {
  /* This is an abstract type so this property saves the type of this typed union */
  plc4c_s7_read_write_s7_payload_type _type;
  /* Properties */
  union {
    struct { /* S7PayloadReadVarResponse */
      plc4c_list* s7_payload_read_var_response_items;
    };
    struct { /* S7PayloadWriteVarRequest */
      plc4c_list* s7_payload_write_var_request_items;
    };
    struct { /* S7PayloadWriteVarResponse */
      plc4c_list* s7_payload_write_var_response_items;
    };
    struct { /* S7PayloadUserData */
      plc4c_list* s7_payload_user_data_items;
    };
  };
};
typedef struct plc4c_s7_read_write_s7_payload plc4c_s7_read_write_s7_payload;

// Create an empty NULL-struct
plc4c_s7_read_write_s7_payload plc4c_s7_read_write_s7_payload_null();

plc4c_return_code plc4c_s7_read_write_s7_payload_parse(plc4c_spi_read_buffer* buf, uint8_t messageType, plc4c_s7_read_write_s7_parameter* parameter, plc4c_s7_read_write_s7_payload** message);

plc4c_return_code plc4c_s7_read_write_s7_payload_serialize(plc4c_spi_write_buffer* buf, plc4c_s7_read_write_s7_payload* message);

uint8_t plc4c_s7_read_write_s7_payload_length_in_bytes(plc4c_s7_read_write_s7_payload* message);

uint8_t plc4c_s7_read_write_s7_payload_length_in_bits(plc4c_s7_read_write_s7_payload* message);

#ifdef __cplusplus
}
#endif
#endif  // PLC4C_S7_READ_WRITE_S7_PAYLOAD_H_

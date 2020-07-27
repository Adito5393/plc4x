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

#include <stdio.h>
#include <plc4c/spi/evaluation_helper.h>
#include "cotp_parameter.h"

// Array of discriminator values that match the enum type constants.
// (The order is identical to the enum constants so we can use the
// enum constant to directly access a given types discriminator values)
const plc4c_s7_read_write_cotp_parameter_discriminator plc4c_s7_read_write_cotp_parameter_discriminators[] = {
  {/* plc4c_s7_read_write_cotp_parameter_tpdu_size */
   .parameterType = 0xC0},
  {/* plc4c_s7_read_write_cotp_parameter_calling_tsap */
   .parameterType = 0xC1},
  {/* plc4c_s7_read_write_cotp_parameter_called_tsap */
   .parameterType = 0xC2},
  {/* plc4c_s7_read_write_cotp_parameter_checksum */
   .parameterType = 0xC3},
  {/* plc4c_s7_read_write_cotp_parameter_disconnect_additional_information */
   .parameterType = 0xE0}
};

// Function returning the discriminator values for a given type constant.
plc4c_s7_read_write_cotp_parameter_discriminator plc4c_s7_read_write_cotp_parameter_get_discriminator(plc4c_s7_read_write_cotp_parameter_type type) {
  return plc4c_s7_read_write_cotp_parameter_discriminators[type];
}

// Create an empty NULL-struct
static const plc4c_s7_read_write_cotp_parameter plc4c_s7_read_write_cotp_parameter_null_const;

plc4c_s7_read_write_cotp_parameter plc4c_s7_read_write_cotp_parameter_null() {
  return plc4c_s7_read_write_cotp_parameter_null_const;
}


// Parse function.
plc4c_return_code plc4c_s7_read_write_cotp_parameter_parse(plc4c_spi_read_buffer* buf, uint8_t rest, plc4c_s7_read_write_cotp_parameter** _message) {
  uint16_t startPos = plc4c_spi_read_get_pos(buf);
  uint16_t curPos;
  plc4c_return_code _res = OK;

  // Allocate enough memory to contain this data structure.
  (*_message) = malloc(sizeof(plc4c_s7_read_write_cotp_parameter));
  if(*_message == NULL) {
    return NO_MEMORY;
  }

  // Discriminator Field (parameterType) (Used as input to a switch field)
  uint8_t parameterType = 0;
  _res = plc4c_spi_read_unsigned_byte(buf, 8, (uint8_t*) &parameterType);
  if(_res != OK) {
    return _res;
  }

  // Implicit Field (parameterLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  uint8_t parameterLength = 0;
  _res = plc4c_spi_read_unsigned_byte(buf, 8, (uint8_t*) &parameterLength);
  if(_res != OK) {
    return _res;
  }

  // Switch Field (Depending on the discriminator values, passes the instantiation to a sub-type)
  if(parameterType == 0xC0) { /* COTPParameterTpduSize */
    (*_message)->_type = plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_tpdu_size;
                    
    // Enum field (tpduSize)
    plc4c_s7_read_write_cotp_tpdu_size tpduSize = plc4c_s7_read_write_cotp_tpdu_size_null();
    _res = plc4c_spi_read_signed_byte(buf, 8, (int8_t*) &tpduSize);
    if(_res != OK) {
      return _res;
    }
    (*_message)->cotp_parameter_tpdu_size_tpdu_size = tpduSize;

  } else 
  if(parameterType == 0xC1) { /* COTPParameterCallingTsap */
    (*_message)->_type = plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_calling_tsap;
                    
    // Simple Field (tsapId)
    uint16_t tsapId = 0;
    _res = plc4c_spi_read_unsigned_short(buf, 16, (uint16_t*) &tsapId);
    if(_res != OK) {
      return _res;
    }
    (*_message)->cotp_parameter_calling_tsap_tsap_id = tsapId;

  } else 
  if(parameterType == 0xC2) { /* COTPParameterCalledTsap */
    (*_message)->_type = plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_called_tsap;
                    
    // Simple Field (tsapId)
    uint16_t tsapId = 0;
    _res = plc4c_spi_read_unsigned_short(buf, 16, (uint16_t*) &tsapId);
    if(_res != OK) {
      return _res;
    }
    (*_message)->cotp_parameter_called_tsap_tsap_id = tsapId;

  } else 
  if(parameterType == 0xC3) { /* COTPParameterChecksum */
    (*_message)->_type = plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_checksum;
                    
    // Simple Field (crc)
    uint8_t crc = 0;
    _res = plc4c_spi_read_unsigned_byte(buf, 8, (uint8_t*) &crc);
    if(_res != OK) {
      return _res;
    }
    (*_message)->cotp_parameter_checksum_crc = crc;

  } else 
  if(parameterType == 0xE0) { /* COTPParameterDisconnectAdditionalInformation */
    (*_message)->_type = plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_disconnect_additional_information;
                    
    // Array field (data)
    plc4c_list* data = NULL;
    plc4c_utils_list_create(&data);
    if(data == NULL) {
      return NO_MEMORY;
    }
    {
      // Count array
      uint8_t itemCount = rest;
      for(int curItem = 0; curItem < itemCount; curItem++) {
        
                  
        uint8_t _value = 0;
        _res = plc4c_spi_read_unsigned_byte(buf, 8, (uint8_t*) &_value);
        if(_res != OK) {
          return _res;
        }
        plc4c_utils_list_insert_head_value(data, &_value);
      }
    }
    (*_message)->cotp_parameter_disconnect_additional_information_data = data;

  }

  return OK;
}

plc4c_return_code plc4c_s7_read_write_cotp_parameter_serialize(plc4c_spi_write_buffer* buf, plc4c_s7_read_write_cotp_parameter* _message) {
  plc4c_return_code _res = OK;

  // Discriminator Field (parameterType)
  plc4c_spi_write_unsigned_byte(buf, 8, plc4c_s7_read_write_cotp_parameter_get_discriminator(_message->_type).parameterType);

  // Implicit Field (parameterLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
  _res = plc4c_spi_write_unsigned_byte(buf, 8, (plc4c_s7_read_write_cotp_parameter_length_in_bytes(_message)) - (2));
  if(_res != OK) {
    return _res;
  }

  // Switch Field (Depending of the current type, serialize the sub-type elements)
  switch(_message->_type) {
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_tpdu_size: {

      // Enum field (tpduSize)
      _res = plc4c_spi_write_signed_byte(buf, 8, _message->cotp_parameter_tpdu_size_tpdu_size);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_calling_tsap: {

      // Simple Field (tsapId)
      _res = plc4c_spi_write_unsigned_short(buf, 16, _message->cotp_parameter_calling_tsap_tsap_id);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_called_tsap: {

      // Simple Field (tsapId)
      _res = plc4c_spi_write_unsigned_short(buf, 16, _message->cotp_parameter_called_tsap_tsap_id);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_checksum: {

      // Simple Field (crc)
      _res = plc4c_spi_write_unsigned_byte(buf, 8, _message->cotp_parameter_checksum_crc);
      if(_res != OK) {
        return _res;
      }

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_disconnect_additional_information: {

      // Array field (data)
      {
        uint8_t itemCount = plc4c_utils_list_size(_message->cotp_parameter_disconnect_additional_information_data);
        for(int curItem = 0; curItem < itemCount; curItem++) {

          uint8_t* _value = (uint8_t*) plc4c_utils_list_get_value(_message->cotp_parameter_disconnect_additional_information_data, curItem);
          plc4c_spi_write_unsigned_byte(buf, 8, *_value);
        }
      }

      break;
    }
  }

  return OK;
}

uint8_t plc4c_s7_read_write_cotp_parameter_length_in_bytes(plc4c_s7_read_write_cotp_parameter* _message) {
  return plc4c_s7_read_write_cotp_parameter_length_in_bits(_message) / 8;
}

uint8_t plc4c_s7_read_write_cotp_parameter_length_in_bits(plc4c_s7_read_write_cotp_parameter* _message) {
  uint8_t lengthInBits = 0;

  // Discriminator Field (parameterType)
  lengthInBits += 8;

  // Implicit Field (parameterLength)
  lengthInBits += 8;

  // Depending of the current type, add the length of sub-type elements ...
  switch(_message->_type) {
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_tpdu_size: {

      // Enum Field (tpduSize)
      lengthInBits += 8;

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_calling_tsap: {

      // Simple field (tsapId)
      lengthInBits += 16;

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_called_tsap: {

      // Simple field (tsapId)
      lengthInBits += 16;

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_checksum: {

      // Simple field (crc)
      lengthInBits += 8;

      break;
    }
    case plc4c_s7_read_write_cotp_parameter_type_plc4c_s7_read_write_cotp_parameter_disconnect_additional_information: {

      // Array field
      lengthInBits += 8 * plc4c_utils_list_size(_message->cotp_parameter_disconnect_additional_information_data);

      break;
    }
  }

  return lengthInBits;
}


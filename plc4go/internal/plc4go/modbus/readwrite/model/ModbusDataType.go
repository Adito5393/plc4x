//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
package model

import (
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
)

// Code generated by build-utils. DO NOT EDIT.

type ModbusDataType uint8

type IModbusDataType interface {
	DataTypeSize() uint8
	Serialize(io utils.WriteBuffer) error
}

const (
	ModbusDataType_BOOL           ModbusDataType = 1
	ModbusDataType_BYTE           ModbusDataType = 2
	ModbusDataType_WORD           ModbusDataType = 3
	ModbusDataType_DWORD          ModbusDataType = 4
	ModbusDataType_LWORD          ModbusDataType = 5
	ModbusDataType_SINT           ModbusDataType = 6
	ModbusDataType_INT            ModbusDataType = 7
	ModbusDataType_DINT           ModbusDataType = 8
	ModbusDataType_LINT           ModbusDataType = 9
	ModbusDataType_USINT          ModbusDataType = 10
	ModbusDataType_UINT           ModbusDataType = 11
	ModbusDataType_UDINT          ModbusDataType = 12
	ModbusDataType_ULINT          ModbusDataType = 13
	ModbusDataType_REAL           ModbusDataType = 14
	ModbusDataType_LREAL          ModbusDataType = 15
	ModbusDataType_TIME           ModbusDataType = 16
	ModbusDataType_LTIME          ModbusDataType = 17
	ModbusDataType_DATE           ModbusDataType = 18
	ModbusDataType_LDATE          ModbusDataType = 19
	ModbusDataType_TIME_OF_DAY    ModbusDataType = 20
	ModbusDataType_LTIME_OF_DAY   ModbusDataType = 21
	ModbusDataType_DATE_AND_TIME  ModbusDataType = 22
	ModbusDataType_LDATE_AND_TIME ModbusDataType = 23
	ModbusDataType_CHAR           ModbusDataType = 24
	ModbusDataType_WCHAR          ModbusDataType = 25
	ModbusDataType_STRING         ModbusDataType = 26
	ModbusDataType_WSTRING        ModbusDataType = 27
)

func (e ModbusDataType) DataTypeSize() uint8 {
	switch e {
	case 1:
		{ /* '1' */
			return 1
		}
	case 10:
		{ /* '10' */
			return 1
		}
	case 11:
		{ /* '11' */
			return 2
		}
	case 12:
		{ /* '12' */
			return 4
		}
	case 13:
		{ /* '13' */
			return 8
		}
	case 14:
		{ /* '14' */
			return 4
		}
	case 15:
		{ /* '15' */
			return 8
		}
	case 16:
		{ /* '16' */
			return 8
		}
	case 17:
		{ /* '17' */
			return 8
		}
	case 18:
		{ /* '18' */
			return 8
		}
	case 19:
		{ /* '19' */
			return 8
		}
	case 2:
		{ /* '2' */
			return 1
		}
	case 20:
		{ /* '20' */
			return 8
		}
	case 21:
		{ /* '21' */
			return 8
		}
	case 22:
		{ /* '22' */
			return 8
		}
	case 23:
		{ /* '23' */
			return 8
		}
	case 24:
		{ /* '24' */
			return 1
		}
	case 25:
		{ /* '25' */
			return 2
		}
	case 26:
		{ /* '26' */
			return 1
		}
	case 27:
		{ /* '27' */
			return 2
		}
	case 3:
		{ /* '3' */
			return 2
		}
	case 4:
		{ /* '4' */
			return 4
		}
	case 5:
		{ /* '5' */
			return 8
		}
	case 6:
		{ /* '6' */
			return 1
		}
	case 7:
		{ /* '7' */
			return 2
		}
	case 8:
		{ /* '8' */
			return 4
		}
	case 9:
		{ /* '9' */
			return 8
		}
	default:
		{
			return 0
		}
	}
}
func ModbusDataTypeByValue(value uint8) ModbusDataType {
	switch value {
	case 1:
		return ModbusDataType_BOOL
	case 10:
		return ModbusDataType_USINT
	case 11:
		return ModbusDataType_UINT
	case 12:
		return ModbusDataType_UDINT
	case 13:
		return ModbusDataType_ULINT
	case 14:
		return ModbusDataType_REAL
	case 15:
		return ModbusDataType_LREAL
	case 16:
		return ModbusDataType_TIME
	case 17:
		return ModbusDataType_LTIME
	case 18:
		return ModbusDataType_DATE
	case 19:
		return ModbusDataType_LDATE
	case 2:
		return ModbusDataType_BYTE
	case 20:
		return ModbusDataType_TIME_OF_DAY
	case 21:
		return ModbusDataType_LTIME_OF_DAY
	case 22:
		return ModbusDataType_DATE_AND_TIME
	case 23:
		return ModbusDataType_LDATE_AND_TIME
	case 24:
		return ModbusDataType_CHAR
	case 25:
		return ModbusDataType_WCHAR
	case 26:
		return ModbusDataType_STRING
	case 27:
		return ModbusDataType_WSTRING
	case 3:
		return ModbusDataType_WORD
	case 4:
		return ModbusDataType_DWORD
	case 5:
		return ModbusDataType_LWORD
	case 6:
		return ModbusDataType_SINT
	case 7:
		return ModbusDataType_INT
	case 8:
		return ModbusDataType_DINT
	case 9:
		return ModbusDataType_LINT
	}
	return 0
}

func ModbusDataTypeByName(value string) ModbusDataType {
	switch value {
	case "BOOL":
		return ModbusDataType_BOOL
	case "USINT":
		return ModbusDataType_USINT
	case "UINT":
		return ModbusDataType_UINT
	case "UDINT":
		return ModbusDataType_UDINT
	case "ULINT":
		return ModbusDataType_ULINT
	case "REAL":
		return ModbusDataType_REAL
	case "LREAL":
		return ModbusDataType_LREAL
	case "TIME":
		return ModbusDataType_TIME
	case "LTIME":
		return ModbusDataType_LTIME
	case "DATE":
		return ModbusDataType_DATE
	case "LDATE":
		return ModbusDataType_LDATE
	case "BYTE":
		return ModbusDataType_BYTE
	case "TIME_OF_DAY":
		return ModbusDataType_TIME_OF_DAY
	case "LTIME_OF_DAY":
		return ModbusDataType_LTIME_OF_DAY
	case "DATE_AND_TIME":
		return ModbusDataType_DATE_AND_TIME
	case "LDATE_AND_TIME":
		return ModbusDataType_LDATE_AND_TIME
	case "CHAR":
		return ModbusDataType_CHAR
	case "WCHAR":
		return ModbusDataType_WCHAR
	case "STRING":
		return ModbusDataType_STRING
	case "WSTRING":
		return ModbusDataType_WSTRING
	case "WORD":
		return ModbusDataType_WORD
	case "DWORD":
		return ModbusDataType_DWORD
	case "LWORD":
		return ModbusDataType_LWORD
	case "SINT":
		return ModbusDataType_SINT
	case "INT":
		return ModbusDataType_INT
	case "DINT":
		return ModbusDataType_DINT
	case "LINT":
		return ModbusDataType_LINT
	}
	return 0
}

func CastModbusDataType(structType interface{}) ModbusDataType {
	castFunc := func(typ interface{}) ModbusDataType {
		if sModbusDataType, ok := typ.(ModbusDataType); ok {
			return sModbusDataType
		}
		return 0
	}
	return castFunc(structType)
}

func (m ModbusDataType) LengthInBits() uint16 {
	return 8
}

func (m ModbusDataType) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func ModbusDataTypeParse(io *utils.ReadBuffer) (ModbusDataType, error) {
	val, err := io.ReadUint8(8)
	if err != nil {
		return 0, nil
	}
	return ModbusDataTypeByValue(val), nil
}

func (e ModbusDataType) Serialize(io utils.WriteBuffer) error {
	err := io.WriteUint8(8, uint8(e))
	return err
}

func (e ModbusDataType) String() string {
	switch e {
	case ModbusDataType_BOOL:
		return "BOOL"
	case ModbusDataType_USINT:
		return "USINT"
	case ModbusDataType_UINT:
		return "UINT"
	case ModbusDataType_UDINT:
		return "UDINT"
	case ModbusDataType_ULINT:
		return "ULINT"
	case ModbusDataType_REAL:
		return "REAL"
	case ModbusDataType_LREAL:
		return "LREAL"
	case ModbusDataType_TIME:
		return "TIME"
	case ModbusDataType_LTIME:
		return "LTIME"
	case ModbusDataType_DATE:
		return "DATE"
	case ModbusDataType_LDATE:
		return "LDATE"
	case ModbusDataType_BYTE:
		return "BYTE"
	case ModbusDataType_TIME_OF_DAY:
		return "TIME_OF_DAY"
	case ModbusDataType_LTIME_OF_DAY:
		return "LTIME_OF_DAY"
	case ModbusDataType_DATE_AND_TIME:
		return "DATE_AND_TIME"
	case ModbusDataType_LDATE_AND_TIME:
		return "LDATE_AND_TIME"
	case ModbusDataType_CHAR:
		return "CHAR"
	case ModbusDataType_WCHAR:
		return "WCHAR"
	case ModbusDataType_STRING:
		return "STRING"
	case ModbusDataType_WSTRING:
		return "WSTRING"
	case ModbusDataType_WORD:
		return "WORD"
	case ModbusDataType_DWORD:
		return "DWORD"
	case ModbusDataType_LWORD:
		return "LWORD"
	case ModbusDataType_SINT:
		return "SINT"
	case ModbusDataType_INT:
		return "INT"
	case ModbusDataType_DINT:
		return "DINT"
	case ModbusDataType_LINT:
		return "LINT"
	}
	return ""
}

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
	"encoding/xml"
	"errors"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
	"io"
)

// Code generated by build-utils. DO NOT EDIT.

// The data-structure of this message
type S7ParameterUserDataItemCPUFunctions struct {
	Method                  uint8
	CpuFunctionType         uint8
	CpuFunctionGroup        uint8
	CpuSubfunction          uint8
	SequenceNumber          uint8
	DataUnitReferenceNumber *uint8
	LastDataUnit            *uint8
	ErrorCode               *uint16
	Parent                  *S7ParameterUserDataItem
	IS7ParameterUserDataItemCPUFunctions
}

// The corresponding interface
type IS7ParameterUserDataItemCPUFunctions interface {
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

///////////////////////////////////////////////////////////
// Accessors for discriminator values.
///////////////////////////////////////////////////////////
func (m *S7ParameterUserDataItemCPUFunctions) ItemType() uint8 {
	return 0x12
}

func (m *S7ParameterUserDataItemCPUFunctions) InitializeParent(parent *S7ParameterUserDataItem) {
}

func NewS7ParameterUserDataItemCPUFunctions(method uint8, cpuFunctionType uint8, cpuFunctionGroup uint8, cpuSubfunction uint8, sequenceNumber uint8, dataUnitReferenceNumber *uint8, lastDataUnit *uint8, errorCode *uint16) *S7ParameterUserDataItem {
	child := &S7ParameterUserDataItemCPUFunctions{
		Method:                  method,
		CpuFunctionType:         cpuFunctionType,
		CpuFunctionGroup:        cpuFunctionGroup,
		CpuSubfunction:          cpuSubfunction,
		SequenceNumber:          sequenceNumber,
		DataUnitReferenceNumber: dataUnitReferenceNumber,
		LastDataUnit:            lastDataUnit,
		ErrorCode:               errorCode,
		Parent:                  NewS7ParameterUserDataItem(),
	}
	child.Parent.Child = child
	return child.Parent
}

func CastS7ParameterUserDataItemCPUFunctions(structType interface{}) *S7ParameterUserDataItemCPUFunctions {
	castFunc := func(typ interface{}) *S7ParameterUserDataItemCPUFunctions {
		if casted, ok := typ.(S7ParameterUserDataItemCPUFunctions); ok {
			return &casted
		}
		if casted, ok := typ.(*S7ParameterUserDataItemCPUFunctions); ok {
			return casted
		}
		if casted, ok := typ.(S7ParameterUserDataItem); ok {
			return CastS7ParameterUserDataItemCPUFunctions(casted.Child)
		}
		if casted, ok := typ.(*S7ParameterUserDataItem); ok {
			return CastS7ParameterUserDataItemCPUFunctions(casted.Child)
		}
		return nil
	}
	return castFunc(structType)
}

func (m *S7ParameterUserDataItemCPUFunctions) GetTypeName() string {
	return "S7ParameterUserDataItemCPUFunctions"
}

func (m *S7ParameterUserDataItemCPUFunctions) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Implicit Field (itemLength)
	lengthInBits += 8

	// Simple field (method)
	lengthInBits += 8

	// Simple field (cpuFunctionType)
	lengthInBits += 4

	// Simple field (cpuFunctionGroup)
	lengthInBits += 4

	// Simple field (cpuSubfunction)
	lengthInBits += 8

	// Simple field (sequenceNumber)
	lengthInBits += 8

	// Optional Field (dataUnitReferenceNumber)
	if m.DataUnitReferenceNumber != nil {
		lengthInBits += 8
	}

	// Optional Field (lastDataUnit)
	if m.LastDataUnit != nil {
		lengthInBits += 8
	}

	// Optional Field (errorCode)
	if m.ErrorCode != nil {
		lengthInBits += 16
	}

	return lengthInBits
}

func (m *S7ParameterUserDataItemCPUFunctions) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func S7ParameterUserDataItemCPUFunctionsParse(io *utils.ReadBuffer) (*S7ParameterUserDataItem, error) {

	// Implicit Field (itemLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
	_, _itemLengthErr := io.ReadUint8(8)
	if _itemLengthErr != nil {
		return nil, errors.New("Error parsing 'itemLength' field " + _itemLengthErr.Error())
	}

	// Simple Field (method)
	method, _methodErr := io.ReadUint8(8)
	if _methodErr != nil {
		return nil, errors.New("Error parsing 'method' field " + _methodErr.Error())
	}

	// Simple Field (cpuFunctionType)
	cpuFunctionType, _cpuFunctionTypeErr := io.ReadUint8(4)
	if _cpuFunctionTypeErr != nil {
		return nil, errors.New("Error parsing 'cpuFunctionType' field " + _cpuFunctionTypeErr.Error())
	}

	// Simple Field (cpuFunctionGroup)
	cpuFunctionGroup, _cpuFunctionGroupErr := io.ReadUint8(4)
	if _cpuFunctionGroupErr != nil {
		return nil, errors.New("Error parsing 'cpuFunctionGroup' field " + _cpuFunctionGroupErr.Error())
	}

	// Simple Field (cpuSubfunction)
	cpuSubfunction, _cpuSubfunctionErr := io.ReadUint8(8)
	if _cpuSubfunctionErr != nil {
		return nil, errors.New("Error parsing 'cpuSubfunction' field " + _cpuSubfunctionErr.Error())
	}

	// Simple Field (sequenceNumber)
	sequenceNumber, _sequenceNumberErr := io.ReadUint8(8)
	if _sequenceNumberErr != nil {
		return nil, errors.New("Error parsing 'sequenceNumber' field " + _sequenceNumberErr.Error())
	}

	// Optional Field (dataUnitReferenceNumber) (Can be skipped, if a given expression evaluates to false)
	var dataUnitReferenceNumber *uint8 = nil
	if bool((cpuFunctionType) == (8)) {
		_val, _err := io.ReadUint8(8)
		if _err != nil {
			return nil, errors.New("Error parsing 'dataUnitReferenceNumber' field " + _err.Error())
		}
		dataUnitReferenceNumber = &_val
	}

	// Optional Field (lastDataUnit) (Can be skipped, if a given expression evaluates to false)
	var lastDataUnit *uint8 = nil
	if bool((cpuFunctionType) == (8)) {
		_val, _err := io.ReadUint8(8)
		if _err != nil {
			return nil, errors.New("Error parsing 'lastDataUnit' field " + _err.Error())
		}
		lastDataUnit = &_val
	}

	// Optional Field (errorCode) (Can be skipped, if a given expression evaluates to false)
	var errorCode *uint16 = nil
	if bool((cpuFunctionType) == (8)) {
		_val, _err := io.ReadUint16(16)
		if _err != nil {
			return nil, errors.New("Error parsing 'errorCode' field " + _err.Error())
		}
		errorCode = &_val
	}

	// Create a partially initialized instance
	_child := &S7ParameterUserDataItemCPUFunctions{
		Method:                  method,
		CpuFunctionType:         cpuFunctionType,
		CpuFunctionGroup:        cpuFunctionGroup,
		CpuSubfunction:          cpuSubfunction,
		SequenceNumber:          sequenceNumber,
		DataUnitReferenceNumber: dataUnitReferenceNumber,
		LastDataUnit:            lastDataUnit,
		ErrorCode:               errorCode,
		Parent:                  &S7ParameterUserDataItem{},
	}
	_child.Parent.Child = _child
	return _child.Parent, nil
}

func (m *S7ParameterUserDataItemCPUFunctions) Serialize(io utils.WriteBuffer) error {
	ser := func() error {

		// Implicit Field (itemLength) (Used for parsing, but it's value is not stored as it's implicitly given by the objects content)
		itemLength := uint8(uint8(uint8(m.LengthInBytes())) - uint8(uint8(2)))
		_itemLengthErr := io.WriteUint8(8, (itemLength))
		if _itemLengthErr != nil {
			return errors.New("Error serializing 'itemLength' field " + _itemLengthErr.Error())
		}

		// Simple Field (method)
		method := uint8(m.Method)
		_methodErr := io.WriteUint8(8, (method))
		if _methodErr != nil {
			return errors.New("Error serializing 'method' field " + _methodErr.Error())
		}

		// Simple Field (cpuFunctionType)
		cpuFunctionType := uint8(m.CpuFunctionType)
		_cpuFunctionTypeErr := io.WriteUint8(4, (cpuFunctionType))
		if _cpuFunctionTypeErr != nil {
			return errors.New("Error serializing 'cpuFunctionType' field " + _cpuFunctionTypeErr.Error())
		}

		// Simple Field (cpuFunctionGroup)
		cpuFunctionGroup := uint8(m.CpuFunctionGroup)
		_cpuFunctionGroupErr := io.WriteUint8(4, (cpuFunctionGroup))
		if _cpuFunctionGroupErr != nil {
			return errors.New("Error serializing 'cpuFunctionGroup' field " + _cpuFunctionGroupErr.Error())
		}

		// Simple Field (cpuSubfunction)
		cpuSubfunction := uint8(m.CpuSubfunction)
		_cpuSubfunctionErr := io.WriteUint8(8, (cpuSubfunction))
		if _cpuSubfunctionErr != nil {
			return errors.New("Error serializing 'cpuSubfunction' field " + _cpuSubfunctionErr.Error())
		}

		// Simple Field (sequenceNumber)
		sequenceNumber := uint8(m.SequenceNumber)
		_sequenceNumberErr := io.WriteUint8(8, (sequenceNumber))
		if _sequenceNumberErr != nil {
			return errors.New("Error serializing 'sequenceNumber' field " + _sequenceNumberErr.Error())
		}

		// Optional Field (dataUnitReferenceNumber) (Can be skipped, if the value is null)
		var dataUnitReferenceNumber *uint8 = nil
		if m.DataUnitReferenceNumber != nil {
			dataUnitReferenceNumber = m.DataUnitReferenceNumber
			_dataUnitReferenceNumberErr := io.WriteUint8(8, *(dataUnitReferenceNumber))
			if _dataUnitReferenceNumberErr != nil {
				return errors.New("Error serializing 'dataUnitReferenceNumber' field " + _dataUnitReferenceNumberErr.Error())
			}
		}

		// Optional Field (lastDataUnit) (Can be skipped, if the value is null)
		var lastDataUnit *uint8 = nil
		if m.LastDataUnit != nil {
			lastDataUnit = m.LastDataUnit
			_lastDataUnitErr := io.WriteUint8(8, *(lastDataUnit))
			if _lastDataUnitErr != nil {
				return errors.New("Error serializing 'lastDataUnit' field " + _lastDataUnitErr.Error())
			}
		}

		// Optional Field (errorCode) (Can be skipped, if the value is null)
		var errorCode *uint16 = nil
		if m.ErrorCode != nil {
			errorCode = m.ErrorCode
			_errorCodeErr := io.WriteUint16(16, *(errorCode))
			if _errorCodeErr != nil {
				return errors.New("Error serializing 'errorCode' field " + _errorCodeErr.Error())
			}
		}

		return nil
	}
	return m.Parent.SerializeParent(io, m, ser)
}

func (m *S7ParameterUserDataItemCPUFunctions) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var token xml.Token
	var err error
	token = start
	for {
		switch token.(type) {
		case xml.StartElement:
			tok := token.(xml.StartElement)
			switch tok.Name.Local {
			case "method":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.Method = data
			case "cpuFunctionType":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.CpuFunctionType = data
			case "cpuFunctionGroup":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.CpuFunctionGroup = data
			case "cpuSubfunction":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.CpuSubfunction = data
			case "sequenceNumber":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.SequenceNumber = data
			case "dataUnitReferenceNumber":
				var data *uint8
				if err := d.DecodeElement(data, &tok); err != nil {
					return err
				}
				m.DataUnitReferenceNumber = data
			case "lastDataUnit":
				var data *uint8
				if err := d.DecodeElement(data, &tok); err != nil {
					return err
				}
				m.LastDataUnit = data
			case "errorCode":
				var data *uint16
				if err := d.DecodeElement(data, &tok); err != nil {
					return err
				}
				m.ErrorCode = data
			}
		}
		token, err = d.Token()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func (m *S7ParameterUserDataItemCPUFunctions) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeElement(m.Method, xml.StartElement{Name: xml.Name{Local: "method"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.CpuFunctionType, xml.StartElement{Name: xml.Name{Local: "cpuFunctionType"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.CpuFunctionGroup, xml.StartElement{Name: xml.Name{Local: "cpuFunctionGroup"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.CpuSubfunction, xml.StartElement{Name: xml.Name{Local: "cpuSubfunction"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.SequenceNumber, xml.StartElement{Name: xml.Name{Local: "sequenceNumber"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.DataUnitReferenceNumber, xml.StartElement{Name: xml.Name{Local: "dataUnitReferenceNumber"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.LastDataUnit, xml.StartElement{Name: xml.Name{Local: "lastDataUnit"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.ErrorCode, xml.StartElement{Name: xml.Name{Local: "errorCode"}}); err != nil {
		return err
	}
	return nil
}

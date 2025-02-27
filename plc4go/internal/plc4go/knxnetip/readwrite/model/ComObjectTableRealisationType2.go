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
type ComObjectTableRealisationType2 struct {
	NumEntries           uint8
	RamFlagsTablePointer uint8
	ComObjectDescriptors []*GroupObjectDescriptorRealisationType2
	Parent               *ComObjectTable
	IComObjectTableRealisationType2
}

// The corresponding interface
type IComObjectTableRealisationType2 interface {
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

///////////////////////////////////////////////////////////
// Accessors for discriminator values.
///////////////////////////////////////////////////////////
func (m *ComObjectTableRealisationType2) FirmwareType() FirmwareType {
	return FirmwareType_SYSTEM_2
}

func (m *ComObjectTableRealisationType2) InitializeParent(parent *ComObjectTable) {
}

func NewComObjectTableRealisationType2(numEntries uint8, ramFlagsTablePointer uint8, comObjectDescriptors []*GroupObjectDescriptorRealisationType2) *ComObjectTable {
	child := &ComObjectTableRealisationType2{
		NumEntries:           numEntries,
		RamFlagsTablePointer: ramFlagsTablePointer,
		ComObjectDescriptors: comObjectDescriptors,
		Parent:               NewComObjectTable(),
	}
	child.Parent.Child = child
	return child.Parent
}

func CastComObjectTableRealisationType2(structType interface{}) *ComObjectTableRealisationType2 {
	castFunc := func(typ interface{}) *ComObjectTableRealisationType2 {
		if casted, ok := typ.(ComObjectTableRealisationType2); ok {
			return &casted
		}
		if casted, ok := typ.(*ComObjectTableRealisationType2); ok {
			return casted
		}
		if casted, ok := typ.(ComObjectTable); ok {
			return CastComObjectTableRealisationType2(casted.Child)
		}
		if casted, ok := typ.(*ComObjectTable); ok {
			return CastComObjectTableRealisationType2(casted.Child)
		}
		return nil
	}
	return castFunc(structType)
}

func (m *ComObjectTableRealisationType2) GetTypeName() string {
	return "ComObjectTableRealisationType2"
}

func (m *ComObjectTableRealisationType2) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Simple field (numEntries)
	lengthInBits += 8

	// Simple field (ramFlagsTablePointer)
	lengthInBits += 8

	// Array field
	if len(m.ComObjectDescriptors) > 0 {
		for _, element := range m.ComObjectDescriptors {
			lengthInBits += element.LengthInBits()
		}
	}

	return lengthInBits
}

func (m *ComObjectTableRealisationType2) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func ComObjectTableRealisationType2Parse(io *utils.ReadBuffer) (*ComObjectTable, error) {

	// Simple Field (numEntries)
	numEntries, _numEntriesErr := io.ReadUint8(8)
	if _numEntriesErr != nil {
		return nil, errors.New("Error parsing 'numEntries' field " + _numEntriesErr.Error())
	}

	// Simple Field (ramFlagsTablePointer)
	ramFlagsTablePointer, _ramFlagsTablePointerErr := io.ReadUint8(8)
	if _ramFlagsTablePointerErr != nil {
		return nil, errors.New("Error parsing 'ramFlagsTablePointer' field " + _ramFlagsTablePointerErr.Error())
	}

	// Array field (comObjectDescriptors)
	// Count array
	comObjectDescriptors := make([]*GroupObjectDescriptorRealisationType2, numEntries)
	for curItem := uint16(0); curItem < uint16(numEntries); curItem++ {
		_item, _err := GroupObjectDescriptorRealisationType2Parse(io)
		if _err != nil {
			return nil, errors.New("Error parsing 'comObjectDescriptors' field " + _err.Error())
		}
		comObjectDescriptors[curItem] = _item
	}

	// Create a partially initialized instance
	_child := &ComObjectTableRealisationType2{
		NumEntries:           numEntries,
		RamFlagsTablePointer: ramFlagsTablePointer,
		ComObjectDescriptors: comObjectDescriptors,
		Parent:               &ComObjectTable{},
	}
	_child.Parent.Child = _child
	return _child.Parent, nil
}

func (m *ComObjectTableRealisationType2) Serialize(io utils.WriteBuffer) error {
	ser := func() error {

		// Simple Field (numEntries)
		numEntries := uint8(m.NumEntries)
		_numEntriesErr := io.WriteUint8(8, (numEntries))
		if _numEntriesErr != nil {
			return errors.New("Error serializing 'numEntries' field " + _numEntriesErr.Error())
		}

		// Simple Field (ramFlagsTablePointer)
		ramFlagsTablePointer := uint8(m.RamFlagsTablePointer)
		_ramFlagsTablePointerErr := io.WriteUint8(8, (ramFlagsTablePointer))
		if _ramFlagsTablePointerErr != nil {
			return errors.New("Error serializing 'ramFlagsTablePointer' field " + _ramFlagsTablePointerErr.Error())
		}

		// Array Field (comObjectDescriptors)
		if m.ComObjectDescriptors != nil {
			for _, _element := range m.ComObjectDescriptors {
				_elementErr := _element.Serialize(io)
				if _elementErr != nil {
					return errors.New("Error serializing 'comObjectDescriptors' field " + _elementErr.Error())
				}
			}
		}

		return nil
	}
	return m.Parent.SerializeParent(io, m, ser)
}

func (m *ComObjectTableRealisationType2) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var token xml.Token
	var err error
	token = start
	for {
		switch token.(type) {
		case xml.StartElement:
			tok := token.(xml.StartElement)
			switch tok.Name.Local {
			case "numEntries":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.NumEntries = data
			case "ramFlagsTablePointer":
				var data uint8
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.RamFlagsTablePointer = data
			case "comObjectDescriptors":
				var data []*GroupObjectDescriptorRealisationType2
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.ComObjectDescriptors = data
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

func (m *ComObjectTableRealisationType2) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeElement(m.NumEntries, xml.StartElement{Name: xml.Name{Local: "numEntries"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.RamFlagsTablePointer, xml.StartElement{Name: xml.Name{Local: "ramFlagsTablePointer"}}); err != nil {
		return err
	}
	if err := e.EncodeToken(xml.StartElement{Name: xml.Name{Local: "comObjectDescriptors"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.ComObjectDescriptors, xml.StartElement{Name: xml.Name{Local: "comObjectDescriptors"}}); err != nil {
		return err
	}
	if err := e.EncodeToken(xml.EndElement{Name: xml.Name{Local: "comObjectDescriptors"}}); err != nil {
		return err
	}
	return nil
}

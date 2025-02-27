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
type AdsMultiRequestItemReadWrite struct {
	ItemIndexGroup  uint32
	ItemIndexOffset uint32
	ItemReadLength  uint32
	ItemWriteLength uint32
	Parent          *AdsMultiRequestItem
	IAdsMultiRequestItemReadWrite
}

// The corresponding interface
type IAdsMultiRequestItemReadWrite interface {
	LengthInBytes() uint16
	LengthInBits() uint16
	Serialize(io utils.WriteBuffer) error
	xml.Marshaler
}

///////////////////////////////////////////////////////////
// Accessors for discriminator values.
///////////////////////////////////////////////////////////
func (m *AdsMultiRequestItemReadWrite) IndexGroup() uint32 {
	return 61570
}

func (m *AdsMultiRequestItemReadWrite) InitializeParent(parent *AdsMultiRequestItem) {
}

func NewAdsMultiRequestItemReadWrite(itemIndexGroup uint32, itemIndexOffset uint32, itemReadLength uint32, itemWriteLength uint32) *AdsMultiRequestItem {
	child := &AdsMultiRequestItemReadWrite{
		ItemIndexGroup:  itemIndexGroup,
		ItemIndexOffset: itemIndexOffset,
		ItemReadLength:  itemReadLength,
		ItemWriteLength: itemWriteLength,
		Parent:          NewAdsMultiRequestItem(),
	}
	child.Parent.Child = child
	return child.Parent
}

func CastAdsMultiRequestItemReadWrite(structType interface{}) *AdsMultiRequestItemReadWrite {
	castFunc := func(typ interface{}) *AdsMultiRequestItemReadWrite {
		if casted, ok := typ.(AdsMultiRequestItemReadWrite); ok {
			return &casted
		}
		if casted, ok := typ.(*AdsMultiRequestItemReadWrite); ok {
			return casted
		}
		if casted, ok := typ.(AdsMultiRequestItem); ok {
			return CastAdsMultiRequestItemReadWrite(casted.Child)
		}
		if casted, ok := typ.(*AdsMultiRequestItem); ok {
			return CastAdsMultiRequestItemReadWrite(casted.Child)
		}
		return nil
	}
	return castFunc(structType)
}

func (m *AdsMultiRequestItemReadWrite) GetTypeName() string {
	return "AdsMultiRequestItemReadWrite"
}

func (m *AdsMultiRequestItemReadWrite) LengthInBits() uint16 {
	lengthInBits := uint16(0)

	// Simple field (itemIndexGroup)
	lengthInBits += 32

	// Simple field (itemIndexOffset)
	lengthInBits += 32

	// Simple field (itemReadLength)
	lengthInBits += 32

	// Simple field (itemWriteLength)
	lengthInBits += 32

	return lengthInBits
}

func (m *AdsMultiRequestItemReadWrite) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func AdsMultiRequestItemReadWriteParse(io *utils.ReadBuffer) (*AdsMultiRequestItem, error) {

	// Simple Field (itemIndexGroup)
	itemIndexGroup, _itemIndexGroupErr := io.ReadUint32(32)
	if _itemIndexGroupErr != nil {
		return nil, errors.New("Error parsing 'itemIndexGroup' field " + _itemIndexGroupErr.Error())
	}

	// Simple Field (itemIndexOffset)
	itemIndexOffset, _itemIndexOffsetErr := io.ReadUint32(32)
	if _itemIndexOffsetErr != nil {
		return nil, errors.New("Error parsing 'itemIndexOffset' field " + _itemIndexOffsetErr.Error())
	}

	// Simple Field (itemReadLength)
	itemReadLength, _itemReadLengthErr := io.ReadUint32(32)
	if _itemReadLengthErr != nil {
		return nil, errors.New("Error parsing 'itemReadLength' field " + _itemReadLengthErr.Error())
	}

	// Simple Field (itemWriteLength)
	itemWriteLength, _itemWriteLengthErr := io.ReadUint32(32)
	if _itemWriteLengthErr != nil {
		return nil, errors.New("Error parsing 'itemWriteLength' field " + _itemWriteLengthErr.Error())
	}

	// Create a partially initialized instance
	_child := &AdsMultiRequestItemReadWrite{
		ItemIndexGroup:  itemIndexGroup,
		ItemIndexOffset: itemIndexOffset,
		ItemReadLength:  itemReadLength,
		ItemWriteLength: itemWriteLength,
		Parent:          &AdsMultiRequestItem{},
	}
	_child.Parent.Child = _child
	return _child.Parent, nil
}

func (m *AdsMultiRequestItemReadWrite) Serialize(io utils.WriteBuffer) error {
	ser := func() error {

		// Simple Field (itemIndexGroup)
		itemIndexGroup := uint32(m.ItemIndexGroup)
		_itemIndexGroupErr := io.WriteUint32(32, (itemIndexGroup))
		if _itemIndexGroupErr != nil {
			return errors.New("Error serializing 'itemIndexGroup' field " + _itemIndexGroupErr.Error())
		}

		// Simple Field (itemIndexOffset)
		itemIndexOffset := uint32(m.ItemIndexOffset)
		_itemIndexOffsetErr := io.WriteUint32(32, (itemIndexOffset))
		if _itemIndexOffsetErr != nil {
			return errors.New("Error serializing 'itemIndexOffset' field " + _itemIndexOffsetErr.Error())
		}

		// Simple Field (itemReadLength)
		itemReadLength := uint32(m.ItemReadLength)
		_itemReadLengthErr := io.WriteUint32(32, (itemReadLength))
		if _itemReadLengthErr != nil {
			return errors.New("Error serializing 'itemReadLength' field " + _itemReadLengthErr.Error())
		}

		// Simple Field (itemWriteLength)
		itemWriteLength := uint32(m.ItemWriteLength)
		_itemWriteLengthErr := io.WriteUint32(32, (itemWriteLength))
		if _itemWriteLengthErr != nil {
			return errors.New("Error serializing 'itemWriteLength' field " + _itemWriteLengthErr.Error())
		}

		return nil
	}
	return m.Parent.SerializeParent(io, m, ser)
}

func (m *AdsMultiRequestItemReadWrite) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var token xml.Token
	var err error
	token = start
	for {
		switch token.(type) {
		case xml.StartElement:
			tok := token.(xml.StartElement)
			switch tok.Name.Local {
			case "itemIndexGroup":
				var data uint32
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.ItemIndexGroup = data
			case "itemIndexOffset":
				var data uint32
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.ItemIndexOffset = data
			case "itemReadLength":
				var data uint32
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.ItemReadLength = data
			case "itemWriteLength":
				var data uint32
				if err := d.DecodeElement(&data, &tok); err != nil {
					return err
				}
				m.ItemWriteLength = data
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

func (m *AdsMultiRequestItemReadWrite) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeElement(m.ItemIndexGroup, xml.StartElement{Name: xml.Name{Local: "itemIndexGroup"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.ItemIndexOffset, xml.StartElement{Name: xml.Name{Local: "itemIndexOffset"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.ItemReadLength, xml.StartElement{Name: xml.Name{Local: "itemReadLength"}}); err != nil {
		return err
	}
	if err := e.EncodeElement(m.ItemWriteLength, xml.StartElement{Name: xml.Name{Local: "itemWriteLength"}}); err != nil {
		return err
	}
	return nil
}

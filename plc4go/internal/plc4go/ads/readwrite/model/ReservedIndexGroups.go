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

type ReservedIndexGroups uint32

type IReservedIndexGroups interface {
	Serialize(io utils.WriteBuffer) error
}

const (
	ReservedIndexGroups_ADSIGRP_SYMTAB                  ReservedIndexGroups = 0x0000F000
	ReservedIndexGroups_ADSIGRP_SYMNAME                 ReservedIndexGroups = 0x0000F001
	ReservedIndexGroups_ADSIGRP_SYMVAL                  ReservedIndexGroups = 0x0000F002
	ReservedIndexGroups_ADSIGRP_SYM_HNDBYNAME           ReservedIndexGroups = 0x0000F003
	ReservedIndexGroups_ADSIGRP_SYM_VALBYNAME           ReservedIndexGroups = 0x0000F004
	ReservedIndexGroups_ADSIGRP_SYM_VALBYHND            ReservedIndexGroups = 0x0000F005
	ReservedIndexGroups_ADSIGRP_SYM_RELEASEHND          ReservedIndexGroups = 0x0000F006
	ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAME          ReservedIndexGroups = 0x0000F007
	ReservedIndexGroups_ADSIGRP_SYM_VERSION             ReservedIndexGroups = 0x0000F008
	ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAMEEX        ReservedIndexGroups = 0x0000F009
	ReservedIndexGroups_ADSIGRP_SYM_DOWNLOAD            ReservedIndexGroups = 0x0000F00A
	ReservedIndexGroups_ADSIGRP_SYM_UPLOAD              ReservedIndexGroups = 0x0000F00B
	ReservedIndexGroups_ADSIGRP_SYM_UPLOADINFO          ReservedIndexGroups = 0x0000F00C
	ReservedIndexGroups_ADSIGRP_SYMNOTE                 ReservedIndexGroups = 0x0000F010
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIB            ReservedIndexGroups = 0x0000F020
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIX            ReservedIndexGroups = 0x0000F021
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RISIZE          ReservedIndexGroups = 0x0000F025
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOB            ReservedIndexGroups = 0x0000F030
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOX            ReservedIndexGroups = 0x0000F031
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOSIZE         ReservedIndexGroups = 0x0000F035
	ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARI          ReservedIndexGroups = 0x0000F040
	ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARO          ReservedIndexGroups = 0x0000F050
	ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIOB           ReservedIndexGroups = 0x0000F060
	ReservedIndexGroups_ADSIGRP_MULTIPLE_READ           ReservedIndexGroups = 0x0000F080
	ReservedIndexGroups_ADSIGRP_MULTIPLE_WRITE          ReservedIndexGroups = 0x0000F081
	ReservedIndexGroups_ADSIGRP_MULTIPLE_READ_WRITE     ReservedIndexGroups = 0x0000F082
	ReservedIndexGroups_ADSIGRP_MULTIPLE_RELEASE_HANDLE ReservedIndexGroups = 0x0000F083
	ReservedIndexGroups_ADSIGRP_DEVICE_DATA             ReservedIndexGroups = 0x0000F100
	ReservedIndexGroups_ADSIOFFS_DEVDATA_ADSSTATE       ReservedIndexGroups = 0x00000000
	ReservedIndexGroups_ADSIOFFS_DEVDATA_DEVSTATE       ReservedIndexGroups = 0x00000002
)

func ReservedIndexGroupsByValue(value uint32) ReservedIndexGroups {
	switch value {
	case 0x00000000:
		return ReservedIndexGroups_ADSIOFFS_DEVDATA_ADSSTATE
	case 0x00000002:
		return ReservedIndexGroups_ADSIOFFS_DEVDATA_DEVSTATE
	case 0x0000F000:
		return ReservedIndexGroups_ADSIGRP_SYMTAB
	case 0x0000F001:
		return ReservedIndexGroups_ADSIGRP_SYMNAME
	case 0x0000F002:
		return ReservedIndexGroups_ADSIGRP_SYMVAL
	case 0x0000F003:
		return ReservedIndexGroups_ADSIGRP_SYM_HNDBYNAME
	case 0x0000F004:
		return ReservedIndexGroups_ADSIGRP_SYM_VALBYNAME
	case 0x0000F005:
		return ReservedIndexGroups_ADSIGRP_SYM_VALBYHND
	case 0x0000F006:
		return ReservedIndexGroups_ADSIGRP_SYM_RELEASEHND
	case 0x0000F007:
		return ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAME
	case 0x0000F008:
		return ReservedIndexGroups_ADSIGRP_SYM_VERSION
	case 0x0000F009:
		return ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAMEEX
	case 0x0000F00A:
		return ReservedIndexGroups_ADSIGRP_SYM_DOWNLOAD
	case 0x0000F00B:
		return ReservedIndexGroups_ADSIGRP_SYM_UPLOAD
	case 0x0000F00C:
		return ReservedIndexGroups_ADSIGRP_SYM_UPLOADINFO
	case 0x0000F010:
		return ReservedIndexGroups_ADSIGRP_SYMNOTE
	case 0x0000F020:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIB
	case 0x0000F021:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIX
	case 0x0000F025:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RISIZE
	case 0x0000F030:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOB
	case 0x0000F031:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOX
	case 0x0000F035:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOSIZE
	case 0x0000F040:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARI
	case 0x0000F050:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARO
	case 0x0000F060:
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIOB
	case 0x0000F080:
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_READ
	case 0x0000F081:
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_WRITE
	case 0x0000F082:
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_READ_WRITE
	case 0x0000F083:
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_RELEASE_HANDLE
	case 0x0000F100:
		return ReservedIndexGroups_ADSIGRP_DEVICE_DATA
	}
	return 0
}

func ReservedIndexGroupsByName(value string) ReservedIndexGroups {
	switch value {
	case "ADSIOFFS_DEVDATA_ADSSTATE":
		return ReservedIndexGroups_ADSIOFFS_DEVDATA_ADSSTATE
	case "ADSIOFFS_DEVDATA_DEVSTATE":
		return ReservedIndexGroups_ADSIOFFS_DEVDATA_DEVSTATE
	case "ADSIGRP_SYMTAB":
		return ReservedIndexGroups_ADSIGRP_SYMTAB
	case "ADSIGRP_SYMNAME":
		return ReservedIndexGroups_ADSIGRP_SYMNAME
	case "ADSIGRP_SYMVAL":
		return ReservedIndexGroups_ADSIGRP_SYMVAL
	case "ADSIGRP_SYM_HNDBYNAME":
		return ReservedIndexGroups_ADSIGRP_SYM_HNDBYNAME
	case "ADSIGRP_SYM_VALBYNAME":
		return ReservedIndexGroups_ADSIGRP_SYM_VALBYNAME
	case "ADSIGRP_SYM_VALBYHND":
		return ReservedIndexGroups_ADSIGRP_SYM_VALBYHND
	case "ADSIGRP_SYM_RELEASEHND":
		return ReservedIndexGroups_ADSIGRP_SYM_RELEASEHND
	case "ADSIGRP_SYM_INFOBYNAME":
		return ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAME
	case "ADSIGRP_SYM_VERSION":
		return ReservedIndexGroups_ADSIGRP_SYM_VERSION
	case "ADSIGRP_SYM_INFOBYNAMEEX":
		return ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAMEEX
	case "ADSIGRP_SYM_DOWNLOAD":
		return ReservedIndexGroups_ADSIGRP_SYM_DOWNLOAD
	case "ADSIGRP_SYM_UPLOAD":
		return ReservedIndexGroups_ADSIGRP_SYM_UPLOAD
	case "ADSIGRP_SYM_UPLOADINFO":
		return ReservedIndexGroups_ADSIGRP_SYM_UPLOADINFO
	case "ADSIGRP_SYMNOTE":
		return ReservedIndexGroups_ADSIGRP_SYMNOTE
	case "ADSIGRP_IOIMAGE_RWIB":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIB
	case "ADSIGRP_IOIMAGE_RWIX":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIX
	case "ADSIGRP_IOIMAGE_RISIZE":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RISIZE
	case "ADSIGRP_IOIMAGE_RWOB":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOB
	case "ADSIGRP_IOIMAGE_RWOX":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOX
	case "ADSIGRP_IOIMAGE_RWOSIZE":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOSIZE
	case "ADSIGRP_IOIMAGE_CLEARI":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARI
	case "ADSIGRP_IOIMAGE_CLEARO":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARO
	case "ADSIGRP_IOIMAGE_RWIOB":
		return ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIOB
	case "ADSIGRP_MULTIPLE_READ":
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_READ
	case "ADSIGRP_MULTIPLE_WRITE":
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_WRITE
	case "ADSIGRP_MULTIPLE_READ_WRITE":
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_READ_WRITE
	case "ADSIGRP_MULTIPLE_RELEASE_HANDLE":
		return ReservedIndexGroups_ADSIGRP_MULTIPLE_RELEASE_HANDLE
	case "ADSIGRP_DEVICE_DATA":
		return ReservedIndexGroups_ADSIGRP_DEVICE_DATA
	}
	return 0
}

func CastReservedIndexGroups(structType interface{}) ReservedIndexGroups {
	castFunc := func(typ interface{}) ReservedIndexGroups {
		if sReservedIndexGroups, ok := typ.(ReservedIndexGroups); ok {
			return sReservedIndexGroups
		}
		return 0
	}
	return castFunc(structType)
}

func (m ReservedIndexGroups) LengthInBits() uint16 {
	return 32
}

func (m ReservedIndexGroups) LengthInBytes() uint16 {
	return m.LengthInBits() / 8
}

func ReservedIndexGroupsParse(io *utils.ReadBuffer) (ReservedIndexGroups, error) {
	val, err := io.ReadUint32(32)
	if err != nil {
		return 0, nil
	}
	return ReservedIndexGroupsByValue(val), nil
}

func (e ReservedIndexGroups) Serialize(io utils.WriteBuffer) error {
	err := io.WriteUint32(32, uint32(e))
	return err
}

func (e ReservedIndexGroups) String() string {
	switch e {
	case ReservedIndexGroups_ADSIOFFS_DEVDATA_ADSSTATE:
		return "ADSIOFFS_DEVDATA_ADSSTATE"
	case ReservedIndexGroups_ADSIOFFS_DEVDATA_DEVSTATE:
		return "ADSIOFFS_DEVDATA_DEVSTATE"
	case ReservedIndexGroups_ADSIGRP_SYMTAB:
		return "ADSIGRP_SYMTAB"
	case ReservedIndexGroups_ADSIGRP_SYMNAME:
		return "ADSIGRP_SYMNAME"
	case ReservedIndexGroups_ADSIGRP_SYMVAL:
		return "ADSIGRP_SYMVAL"
	case ReservedIndexGroups_ADSIGRP_SYM_HNDBYNAME:
		return "ADSIGRP_SYM_HNDBYNAME"
	case ReservedIndexGroups_ADSIGRP_SYM_VALBYNAME:
		return "ADSIGRP_SYM_VALBYNAME"
	case ReservedIndexGroups_ADSIGRP_SYM_VALBYHND:
		return "ADSIGRP_SYM_VALBYHND"
	case ReservedIndexGroups_ADSIGRP_SYM_RELEASEHND:
		return "ADSIGRP_SYM_RELEASEHND"
	case ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAME:
		return "ADSIGRP_SYM_INFOBYNAME"
	case ReservedIndexGroups_ADSIGRP_SYM_VERSION:
		return "ADSIGRP_SYM_VERSION"
	case ReservedIndexGroups_ADSIGRP_SYM_INFOBYNAMEEX:
		return "ADSIGRP_SYM_INFOBYNAMEEX"
	case ReservedIndexGroups_ADSIGRP_SYM_DOWNLOAD:
		return "ADSIGRP_SYM_DOWNLOAD"
	case ReservedIndexGroups_ADSIGRP_SYM_UPLOAD:
		return "ADSIGRP_SYM_UPLOAD"
	case ReservedIndexGroups_ADSIGRP_SYM_UPLOADINFO:
		return "ADSIGRP_SYM_UPLOADINFO"
	case ReservedIndexGroups_ADSIGRP_SYMNOTE:
		return "ADSIGRP_SYMNOTE"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIB:
		return "ADSIGRP_IOIMAGE_RWIB"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIX:
		return "ADSIGRP_IOIMAGE_RWIX"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RISIZE:
		return "ADSIGRP_IOIMAGE_RISIZE"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOB:
		return "ADSIGRP_IOIMAGE_RWOB"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOX:
		return "ADSIGRP_IOIMAGE_RWOX"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWOSIZE:
		return "ADSIGRP_IOIMAGE_RWOSIZE"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARI:
		return "ADSIGRP_IOIMAGE_CLEARI"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_CLEARO:
		return "ADSIGRP_IOIMAGE_CLEARO"
	case ReservedIndexGroups_ADSIGRP_IOIMAGE_RWIOB:
		return "ADSIGRP_IOIMAGE_RWIOB"
	case ReservedIndexGroups_ADSIGRP_MULTIPLE_READ:
		return "ADSIGRP_MULTIPLE_READ"
	case ReservedIndexGroups_ADSIGRP_MULTIPLE_WRITE:
		return "ADSIGRP_MULTIPLE_WRITE"
	case ReservedIndexGroups_ADSIGRP_MULTIPLE_READ_WRITE:
		return "ADSIGRP_MULTIPLE_READ_WRITE"
	case ReservedIndexGroups_ADSIGRP_MULTIPLE_RELEASE_HANDLE:
		return "ADSIGRP_MULTIPLE_RELEASE_HANDLE"
	case ReservedIndexGroups_ADSIGRP_DEVICE_DATA:
		return "ADSIGRP_DEVICE_DATA"
	}
	return ""
}

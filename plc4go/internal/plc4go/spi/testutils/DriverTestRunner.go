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
package testutils

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	model2 "github.com/apache/plc4x/plc4go/internal/plc4go/modbus/readwrite"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/transports"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/transports/test"
	"github.com/apache/plc4x/plc4go/internal/plc4go/spi/utils"
	"github.com/apache/plc4x/plc4go/pkg/plc4go"
	api "github.com/apache/plc4x/plc4go/pkg/plc4go/model"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/subchen/go-xmldom"
	"os"
	"strconv"
	"testing"
	"time"
)

type DriverTestsuite struct {
	name             string
	driverName       string
	driverParameters map[string]string
	setupSteps       []TestStep
	teardownSteps    []TestStep
	testcases        []Testcase
}

func (m DriverTestsuite) Run(driverManager plc4go.PlcDriverManager, testcase Testcase) error {
	// Get a connection
	connectionChan := driverManager.GetConnection(m.driverName + ":test://hurz")
	connectionResult := <-connectionChan

	if connectionResult.Err != nil {
		return errors.Wrap(connectionResult.Err, "error getting a connection")
	}

	log.Info().Msgf("\n\n-------------------------------------------------------\nExecuting testcase: %s \n", testcase.name)

	// Run the setup steps
	for _, testStep := range m.setupSteps {
		err := m.ExecuteStep(connectionResult.Connection, &testcase, testStep)
		if err != nil {
			return errors.Wrap(err, "error in setup step "+testStep.name)
		}
	}

	// Run the actual scenario steps
	for _, testStep := range testcase.steps {
		err := m.ExecuteStep(connectionResult.Connection, &testcase, testStep)
		if err != nil {
			return errors.Wrap(err, "error in step "+testStep.name)
		}
	}

	// Run the teardown steps
	for _, testStep := range m.teardownSteps {
		err := m.ExecuteStep(connectionResult.Connection, &testcase, testStep)
		if err != nil {
			return errors.Wrap(err, "error in teardown step "+testStep.name)
		}
	}

	log.Info().Msgf("-------------------------------------------------------\nDone\n-------------------------------------------------------\n")
	return nil
}

func (m DriverTestsuite) ExecuteStep(connection plc4go.PlcConnection, testcase *Testcase, step TestStep) error {
	mc, ok := connection.(spi.TransportInstanceExposer)
	if !ok {
		return errors.New("couldn't access connections transport instance")
	}
	testTransportInstance, ok := mc.GetTransportInstance().(transports.TestTransportInstance)
	if !ok {
		return errors.New("transport must be of type TestTransport")
	}

	log.Info().Msgf(" - Executing step: %s \n", step.name)

	switch step.stepType {
	case StepType_API_REQUEST:
		switch step.payload.Name {
		case "TestReadRequest":
			// Assemble a read-request according to the information in the test xml
			rrb := connection.ReadRequestBuilder()
			for _, fieldNode := range step.payload.GetChild("fields").GetChildren("field") {
				fieldName := fieldNode.GetChild("name").Text
				fieldAddress := fieldNode.GetChild("address").Text
				rrb.AddQuery(fieldName, fieldAddress)
			}
			readRequest, err := rrb.Build()
			if err != nil {
				return errors.Wrap(err, "Error creating read-request")
			}

			// Execute the read-request and store the response-channel in the testcase.
			if testcase.readRequestResultChannel != nil {
				return errors.New("testcase read-request result channel already occupied")
			}
			testcase.readRequestResultChannel = readRequest.Execute()
		case "TestWriteRequest":
			wrb := connection.WriteRequestBuilder()
			for _, fieldNode := range step.payload.GetChild("fields").GetChildren("field") {
				fieldName := fieldNode.GetChild("name").Text
				fieldAddress := fieldNode.GetChild("address").Text

				he, ok := connection.(spi.HandlerExposer)
				if !ok {
					return errors.New("connection is not a HandlerExposer")
				}
				field, err := he.GetPlcFieldHandler().ParseQuery(fieldAddress)
				if err != nil {
					return errors.Wrap(err, "error parsing address: "+fieldAddress)
				}
				if field.GetQuantity() > 1 {
					var fieldValue []string
					for _, valueChild := range fieldNode.GetChildren("value") {
						fieldValue = append(fieldValue, valueChild.Text)
					}
					wrb.AddQuery(fieldName, fieldAddress, fieldValue)
				} else {
					fieldValue := fieldNode.GetChild("value").Text
					wrb.AddQuery(fieldName, fieldAddress, fieldValue)
				}
			}
			writeRequest, err := wrb.Build()
			if err != nil {
				return errors.Wrap(err, "Error creating write-request")
			}
			if testcase.writeRequestResultChannel != nil {
				return errors.New("testcase write-request result channel already occupied")
			}
			testcase.writeRequestResultChannel = writeRequest.Execute()
		}
	case StepType_API_RESPONSE:
		switch step.payload.Name {
		case "PlcReadResponse":
			if testcase.readRequestResultChannel == nil {
				return errors.New("no read response expected")
			}
			readRequestResult := <-testcase.readRequestResultChannel
			// Serialize the response to XML
			actualResponse, err := xml.Marshal(readRequestResult.Response)
			if err != nil {
				return errors.Wrap(err, "error serializing response")
			}
			// Get the reference XML
			referenceSerialized := step.payload.XML()
			// Compare the results
			err = CompareResults(actualResponse, []byte(referenceSerialized))
			if err != nil {
				return errors.Wrap(err, "Error comparing the results")
			}
		case "PlcWriteResponse":
			if testcase.writeRequestResultChannel == nil {
				return errors.New("no write response expected")
			}
			writeResponseResult := <-testcase.writeRequestResultChannel
			// Serialize the response to XML
			actualResponse, err := xml.Marshal(writeResponseResult.Response)
			if err != nil {
				return errors.Wrap(err, "error serializing response")
			}
			// Get the reference XML
			referenceSerialized := step.payload.XML()
			// Compare the results
			err = CompareResults(actualResponse, []byte(referenceSerialized))
			if err != nil {
				return errors.Wrap(err, "Error comparing the results")
			}
		}
	case StepType_OUTGOING_PLC_MESSAGE:
		typeName := step.payload.Name
		payloadString := step.payload.XML()

		// Parse the xml into a real model
		message, err := model2.ModbusXmlParserHelper{}.Parse(typeName, payloadString)
		if err != nil {
			return errors.Wrap(err, "error parsing xml")
		}

		// Serialize the model into bytes
		ser, ok := message.(utils.Serializable)
		if !ok {
			return errors.New("error converting type into Serializable type")
		}
		wb := utils.NewWriteBuffer()
		err = ser.Serialize(*wb)
		if err != nil {
			return errors.Wrap(err, "error serializing message")
		}
		expectedRawOutput := wb.GetBytes()
		expectedRawOutputLength := uint32(len(expectedRawOutput))

		// Read exactly this amount of bytes from the transport
		if testTransportInstance.GetNumDrainableBytes() < expectedRawOutputLength {
			return errors.New("error getting bytes from transport. Not enough data available")
		}
		rawOutput, err := testTransportInstance.DrainWriteBuffer(expectedRawOutputLength)
		if err != nil {
			return errors.Wrap(err, "error getting bytes from transport")
		}

		// Compare the bytes read with the ones we expect
		for i := range expectedRawOutput {
			if expectedRawOutput[i] != rawOutput[i] {
				return errors.New("actual output doesn't match expected output")
			}
		}
		// If there's a difference, parse the input and display it to simplify debugging
	case StepType_OUTGOING_PLC_BYTES:
		// Read exactly this amount of bytes from the transport
		expectedRawInput, err := hex.DecodeString(step.payload.Text)
		if err != nil {
			return errors.Wrap(err, "error decoding hex-encoded byte data")
		}
		rawInput, err := testTransportInstance.DrainWriteBuffer(uint32(len(expectedRawInput)))
		if err != nil {
			return errors.Wrap(err, "error getting bytes from transport")
		}

		// Compare the bytes read with the ones we expect
		for i := range expectedRawInput {
			if expectedRawInput[i] != rawInput[i] {
				return errors.New("actual output doesn't match expected output")
			}
		}
		// If there's a difference, parse the input and display it to simplify debugging
	case StepType_INCOMING_PLC_MESSAGE:
		typeName := step.payload.Name
		payloadString := step.payload.XML()

		// Parse the xml into a real model
		message, err := model2.ModbusXmlParserHelper{}.Parse(typeName, payloadString)
		if err != nil {
			return errors.Wrap(err, "error parsing xml")
		}

		// Serialize the model into bytes
		ser, ok := message.(utils.Serializable)
		if !ok {
			return errors.New("error converting type into Serializable type")
		}
		wb := utils.NewWriteBuffer()
		err = ser.Serialize(*wb)
		if err != nil {
			return errors.Wrap(err, "error serializing message")
		}

		// Send these bytes to the transport
		err = testTransportInstance.FillReadBuffer(wb.GetBytes())
		if err != nil {
			return errors.Wrap(err, "error writing data to transport")
		}
	case StepType_INCOMING_PLC_BYTES:
		// Get the raw hex-data.
		rawInput, err := hex.DecodeString(step.payload.Text)
		if err != nil {
			return errors.Wrap(err, "error decoding hex-encoded byte data: ")
		}

		// Send these bytes to the transport
		err = testTransportInstance.FillReadBuffer(rawInput)
		if err != nil {
			return errors.Wrap(err, "error writing data to transport")
		}
	case StepType_DELAY:
		// Get the number of milliseconds
		delay, err := strconv.Atoi(step.payload.Text)
		if err != nil {
			return errors.Wrap(err, "invalid delay format")
		}
		// Sleep for that long
		time.Sleep(time.Duration(delay))
	case StepType_TERMINATE:
		// Simply close the transport connection
		err := testTransportInstance.Close()
		if err != nil {
			return errors.Wrap(err, "error closing transport")
		}
	}
	return nil
}

func (m DriverTestsuite) ParseXml(referenceXml *xmldom.Node, parserArguments []string) {
	normalizeXml(referenceXml)
	//referenceSerialized := referenceXml.FirstChild().XML()
}

type Testcase struct {
	name                      string
	steps                     []TestStep
	readRequestResultChannel  <-chan api.PlcReadRequestResult
	writeRequestResultChannel <-chan api.PlcWriteRequestResult
}

type TestStep struct {
	name            string
	stepType        StepType
	parserArguments []string
	payload         xmldom.Node
}

type StepType uint8

const (
	StepType_OUTGOING_PLC_MESSAGE StepType = 0x01
	StepType_OUTGOING_PLC_BYTES   StepType = 0x02
	StepType_INCOMING_PLC_MESSAGE StepType = 0x03
	StepType_INCOMING_PLC_BYTES   StepType = 0x04
	StepType_API_REQUEST          StepType = 0x05
	StepType_API_RESPONSE         StepType = 0x06
	StepType_DELAY                StepType = 0x07
	StepType_TERMINATE            StepType = 0x08
)

func RunDriverTestsuite(t *testing.T, driver plc4go.PlcDriver, testPath string) {
	// Read the test-specification as XML file
	rootNode, err := ParseDriverTestsuiteXml(testPath)
	if err != nil {
		t.Error(err.Error())
		t.Fail()
		return
	}

	// Parse the contents of the test-specification
	testsuite, err := ParseDriverTestsuite(*rootNode)
	if err != nil {
		// TODO: zerolog doesn't render stack human readable :(
		fmt.Printf("%+v\n", err)
		log.Error().
			Stack().
			Err(err).
			Msg("Failed to parse test-specification")
		t.Error(err)
		t.Fail()
		return
	}

	// Initialize the driver manager
	driverManager := plc4go.NewPlcDriverManager()
	driverManager.RegisterTransport(test.NewTestTransport())
	driverManager.RegisterDriver(driver)

	for _, testcase := range testsuite.testcases {
		err := testsuite.Run(driverManager, testcase)
		if err != nil {
			log.Err(err).Msgf("-------------------------------------------------------\nFailure\n%s\n-------------------------------------------------------\n", err.Error())
			t.Fail()
		}
	}
	// Execute the tests in the testsuite
	log.Info().Msgf(testsuite.name)
}

func ParseDriverTestsuiteXml(testPath string) (*xmldom.Node, error) {
	// Get the current working directory
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// Check if the test-file is available
	info, err := os.Stat(path + "/../../../../" + testPath)
	if os.IsNotExist(err) {
		return nil, errors.Wrap(err, "test-File doesn't exist")
	}
	if info.IsDir() {
		return nil, errors.New("test-file refers to a directory")
	}

	// Open a reader for this file
	dat, err := os.Open(path + "/../../../../" + testPath)
	if err != nil {
		return nil, errors.Wrap(err, "error opening file")
	}

	// Read the xml
	node := xmldom.Must(xmldom.Parse(dat)).Root
	return node, nil
}

func ParseDriverTestsuite(node xmldom.Node) (*DriverTestsuite, error) {
	if node.Name != "driver-testsuite" {
		return nil, errors.New("invalid document structure")
	}
	var testsuiteName string
	var driverName string
	driverParameters := make(map[string]string)
	var setupSteps []TestStep
	var teardownSteps []TestStep
	var testcases []Testcase
	for _, childPtr := range node.Children {
		child := *childPtr
		if child.Name == "name" {
			testsuiteName = child.Text
		} else if child.Name == "driver-name" {
			driverName = child.Text
		} else if child.Name == "driver-parameters" {
			parameterList := child.FindByName("parameter")
			for _, parameter := range parameterList {
				nameElement := parameter.FindOneByName("name")
				valueElement := parameter.FindOneByName("value")
				if nameElement == nil || valueElement == nil {
					return nil, errors.New("invalid parameter found: no present")
				}
				name := nameElement.Text
				value := valueElement.Text
				if name == "" || value == "" {
					return nil, errors.New("invalid parameter found: empty")
				}
				driverParameters[name] = value
			}
		} else if child.Name == "setup" {
			steps, err := ParseDriverTestsuiteSteps(child)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing setup steps")
			}
			setupSteps = steps
		} else if child.Name == "teardown" {
			steps, err := ParseDriverTestsuiteSteps(child)
			if err != nil {
				return nil, errors.Wrap(err, "error teardown setup steps")
			}
			teardownSteps = steps
		} else if child.Name == "testcase" {
			testcaseName := child.FindOneByName("name").Text
			stepsNode := child.FindOneByName("steps")
			steps, err := ParseDriverTestsuiteSteps(*stepsNode)
			if err != nil {
				return nil, errors.Wrap(err, "error parsing testcase "+testcaseName)
			}
			testcase := Testcase{
				name:  testcaseName,
				steps: steps,
			}
			testcases = append(testcases, testcase)
		} else {
			return nil, errors.New("invalid document structure. Unhandled element " + child.Name)
		}
	}
	log.Info().
		Str("testsuite name", testsuiteName).
		Str("driver name", driverName).
		Msgf("Parsed test suite %s", testsuiteName)

	return &DriverTestsuite{
		name:             testsuiteName,
		driverName:       driverName,
		driverParameters: driverParameters,
		setupSteps:       setupSteps,
		teardownSteps:    teardownSteps,
		testcases:        testcases,
	}, nil
}

func ParseDriverTestsuiteSteps(node xmldom.Node) ([]TestStep, error) {
	log.Debug().Str("rootElement", node.Name).Msg("Parsing driver testsuite steps")
	var testSteps []TestStep
	for _, step := range node.Children {
		name := step.GetAttributeValue("name")
		log.Debug().Str("rootElement", node.Name).Str("name", name).Msg("Parsing step")
		var stepType StepType
		switch step.Name {
		case "api-request":
			stepType = StepType_API_REQUEST
		case "api-response":
			stepType = StepType_API_RESPONSE
		case "outgoing-plc-message":
			stepType = StepType_OUTGOING_PLC_MESSAGE
		case "incoming-plc-message":
			stepType = StepType_INCOMING_PLC_MESSAGE
		case "outgoing-plc-bytes":
			stepType = StepType_OUTGOING_PLC_BYTES
		case "incoming-plc-bytes":
			stepType = StepType_INCOMING_PLC_BYTES
		case "delay":
			stepType = StepType_DELAY
		case "terminate":
			stepType = StepType_TERMINATE
		default:
			return nil, errors.Errorf("Unknown step with name %s", step.Name)
		}
		var parserArguments []string
		var payload *xmldom.Node
		log.Debug().Str("rootElement", node.Name).Msg("Looking for payload")
		for _, childNode := range step.Children {
			log.Debug().Str("child node name", childNode.Name).Str("rootElement", node.Name).Msg("Found payload candidate")
			if childNode.Name == "parser-arguments" {
				for _, parserArgumentNode := range childNode.Children {
					parserArguments = append(parserArguments, parserArgumentNode.Text)
				}
			} else if payload == nil {
				payload = childNode
			} else {
				return nil, errors.New("test step can only contain a single payload element")
			}
		}
		if stepType == StepType_DELAY {
			payload = step
		}
		if payload == nil {
			return nil, errors.New("missing payload element")
		}
		testSteps = append(testSteps, TestStep{
			name:            name,
			stepType:        stepType,
			parserArguments: parserArguments,
			payload:         *payload,
		})
	}
	return testSteps, nil
}

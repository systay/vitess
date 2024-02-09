// Code generated by MockGen. DO NOT EDIT.
// Source: vitess.io/vitess/go/vt/vttablet/tabletserver/txthrottler (interfaces: ThrottlerInterface)

// Package txthrottler is a generated GoMock package.
package txthrottler

import (
	reflect "reflect"
	time "time"

	gomock "go.uber.org/mock/gomock"

	discovery "vitess.io/vitess/go/vt/discovery"
	throttlerdata "vitess.io/vitess/go/vt/proto/throttlerdata"
	topodata "vitess.io/vitess/go/vt/proto/topodata"
)

// MockThrottlerInterface is a mock of ThrottlerInterface interface.
type MockThrottlerInterface struct {
	ctrl     *gomock.Controller
	recorder *MockThrottlerInterfaceMockRecorder
}

// MockThrottlerInterfaceMockRecorder is the mock recorder for MockThrottlerInterface.
type MockThrottlerInterfaceMockRecorder struct {
	mock *MockThrottlerInterface
}

// NewMockThrottlerInterface creates a new mock instance.
func NewMockThrottlerInterface(ctrl *gomock.Controller) *MockThrottlerInterface {
	mock := &MockThrottlerInterface{ctrl: ctrl}
	mock.recorder = &MockThrottlerInterfaceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockThrottlerInterface) EXPECT() *MockThrottlerInterfaceMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockThrottlerInterface) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockThrottlerInterfaceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockThrottlerInterface)(nil).Close))
}

// GetConfiguration mocks base method.
func (m *MockThrottlerInterface) GetConfiguration() *throttlerdata.Configuration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetConfiguration")
	ret0, _ := ret[0].(*throttlerdata.Configuration)
	return ret0
}

// GetConfiguration indicates an expected call of GetConfiguration.
func (mr *MockThrottlerInterfaceMockRecorder) GetConfiguration() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetConfiguration", reflect.TypeOf((*MockThrottlerInterface)(nil).GetConfiguration))
}

// MaxLag mocks base method.
func (m *MockThrottlerInterface) MaxLag(tabletType topodata.TabletType) uint32 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxLag", tabletType)
	ret0, _ := ret[0].(uint32)
	return ret0
}

// MaxLag indicates an expected call of LastMaxLagNotIgnoredForTabletType.
func (mr *MockThrottlerInterfaceMockRecorder) MaxLag(tabletType interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxLag", reflect.TypeOf((*MockThrottlerInterface)(nil).MaxLag), tabletType)
}

// MaxRate mocks base method.
func (m *MockThrottlerInterface) MaxRate() int64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxRate")
	ret0, _ := ret[0].(int64)
	return ret0
}

// MaxRate indicates an expected call of MaxRate.
func (mr *MockThrottlerInterfaceMockRecorder) MaxRate() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxRate", reflect.TypeOf((*MockThrottlerInterface)(nil).MaxRate))
}

// RecordReplicationLag mocks base method.
func (m *MockThrottlerInterface) RecordReplicationLag(arg0 time.Time, arg1 *discovery.TabletHealth) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RecordReplicationLag", arg0, arg1)
}

// RecordReplicationLag indicates an expected call of RecordReplicationLag.
func (mr *MockThrottlerInterfaceMockRecorder) RecordReplicationLag(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecordReplicationLag", reflect.TypeOf((*MockThrottlerInterface)(nil).RecordReplicationLag), arg0, arg1)
}

// ResetConfiguration mocks base method.
func (m *MockThrottlerInterface) ResetConfiguration() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ResetConfiguration")
}

// ResetConfiguration indicates an expected call of ResetConfiguration.
func (mr *MockThrottlerInterfaceMockRecorder) ResetConfiguration() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ResetConfiguration", reflect.TypeOf((*MockThrottlerInterface)(nil).ResetConfiguration))
}

// SetMaxRate mocks base method.
func (m *MockThrottlerInterface) SetMaxRate(arg0 int64) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetMaxRate", arg0)
}

// SetMaxRate indicates an expected call of SetMaxRate.
func (mr *MockThrottlerInterfaceMockRecorder) SetMaxRate(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetMaxRate", reflect.TypeOf((*MockThrottlerInterface)(nil).SetMaxRate), arg0)
}

// ThreadFinished mocks base method.
func (m *MockThrottlerInterface) ThreadFinished(arg0 int) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "ThreadFinished", arg0)
}

// ThreadFinished indicates an expected call of ThreadFinished.
func (mr *MockThrottlerInterfaceMockRecorder) ThreadFinished(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ThreadFinished", reflect.TypeOf((*MockThrottlerInterface)(nil).ThreadFinished), arg0)
}

// Throttle mocks base method.
func (m *MockThrottlerInterface) Throttle(arg0 int) time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Throttle", arg0)
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// Throttle indicates an expected call of Throttle.
func (mr *MockThrottlerInterfaceMockRecorder) Throttle(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Throttle", reflect.TypeOf((*MockThrottlerInterface)(nil).Throttle), arg0)
}

// UpdateConfiguration mocks base method.
func (m *MockThrottlerInterface) UpdateConfiguration(arg0 *throttlerdata.Configuration, arg1 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateConfiguration", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateConfiguration indicates an expected call of UpdateConfiguration.
func (mr *MockThrottlerInterfaceMockRecorder) UpdateConfiguration(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateConfiguration", reflect.TypeOf((*MockThrottlerInterface)(nil).UpdateConfiguration), arg0, arg1)
}

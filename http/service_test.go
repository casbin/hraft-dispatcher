package http

import (
	"bytes"
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	jsoniter "github.com/json-iterator/go"
	"github.com/nodece/casbin-hraft-dispatcher/command"
	"github.com/nodece/casbin-hraft-dispatcher/http/mocks"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewService(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)
	s, err := NewService("127.0.0.1:0", nil, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
}

func TestRedirect(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)
	s, err := NewService("127.0.0.1:0", nil, store)
	assert.NoError(t, err)

	r := httptest.NewRequest(http.MethodPut, "https://127.0.0.1:6971/policies/add", nil)
	actualURL := s.getRedirectURL(r, "127.0.0.1:6970")
	expectedURL := "https://127.0.0.1:6970/policies/add"
	assert.Equal(t, expectedURL, actualURL)
}

func TestLeader(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)
	s, err := NewService("127.0.0.1:0", nil, store)
	assert.NoError(t, err)

	store.EXPECT().Leader().Return(true, "127.0.0.1:6790")
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	r := s.leaderMiddleware(nextHandler)
	w := httptest.NewRecorder()
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPut, "https://testing", nil))
	assert.Equal(t, w.Code, http.StatusOK)

	store.EXPECT().Leader().Return(false, "127.0.0.1:6790")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "https://testing", nil))
	assert.Equal(t, w.Header().Get("Location"), "https://127.0.0.1:6790")
	assert.Equal(t, w.Code, http.StatusTemporaryRedirect)
}

func TestAddPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)

	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	addPolicyRequest := &command.AddPolicyRequest{
		Sec:   "p",
		PType: "p",
		Rules: []*command.StringArray{{Items: []string{"role:admin", "/", "*"}}},
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().AddPolicy(addPolicyRequest).Return(nil)

	b, err := jsoniter.Marshal(addPolicyRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/add", s.Addr()), bytes.NewBuffer(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRemovePolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	removePolicyRequest := &command.RemovePolicyRequest{
		Sec:   "p",
		PType: "p",
		Rules: []*command.StringArray{{Items: []string{"role:admin", "/", "*"}}},
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().RemovePolicy(removePolicyRequest).Return(nil)

	b, err := jsoniter.Marshal(removePolicyRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove", s.Addr()), bytes.NewBuffer(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRemoveFilteredPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	removeFilteredPolicyRequest := &command.RemoveFilteredPolicyRequest{
		Sec:         "p",
		PType:       "p",
		FieldIndex:  0,
		FieldValues: []string{"role:admin"},
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().RemoveFilteredPolicy(removeFilteredPolicyRequest).Return(nil)

	b, err := jsoniter.Marshal(removeFilteredPolicyRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove?type=filtered", s.Addr()), bytes.NewBuffer(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestUpdatePolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	updatePolicyRequest := &command.UpdatePolicyRequest{
		Sec:     "p",
		PType:   "p",
		OldRule: []string{"role:admin", "/", "*"},
		NewRule: []string{"role:admin", "/admin", "*"},
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().UpdatePolicy(updatePolicyRequest).Return(nil)

	b, err := jsoniter.Marshal(updatePolicyRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/update", s.Addr()), bytes.NewBuffer(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestClearPolicy(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().ClearPolicy().Return(nil)

	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/policies/remove?type=all", s.Addr()), nil)
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestJoinNode(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	addNodeRequest := &command.AddNodeRequest{
		Id:      "test-main",
		Address: "10.0.7.10",
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().JoinNode(addNodeRequest.Id, addNodeRequest.Address).Return(nil)

	b, err := jsoniter.Marshal(addNodeRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/nodes/join", s.Addr()), bytes.NewReader(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestRemoveNode(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	store := mocks.NewMockStore(ctl)

	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	s, err := NewService("127.0.0.1:0", ts.TLS, store)
	assert.NoError(t, err)
	assert.NotNil(t, s)
	go func() {
		err = s.Start()
		assert.EqualError(t, err, http.ErrServerClosed.Error())
	}()
	defer s.Stop(context.Background())
	<-time.After(1 * time.Second)

	removeNodeRequest := &command.RemoveNodeRequest{
		Id: "test-main",
	}
	store.EXPECT().Leader().Return(true, s.Addr())
	store.EXPECT().RemoveNode(removeNodeRequest.Id).Return(nil)

	b, err := jsoniter.Marshal(removeNodeRequest)
	assert.NoError(t, err)
	r, err := http.NewRequest(http.MethodPut, fmt.Sprintf("https://%s/nodes/remove", s.Addr()), bytes.NewReader(b))
	assert.NoError(t, err)

	resp, err := ts.Client().Do(r)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

package casbin_hraft_dispatcher

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pkg/errors"

	"github.com/golang/mock/gomock"
	"github.com/nodece/casbin-hraft-dispatcher/mock"
	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dispatcherStore := mock.NewMockDispatcherStore(ctrl)
	dispatcherStore.EXPECT().Leader().Return(true, "127.0.0.1").AnyTimes()

	// TODO: move to integration test
	//ts := httptest.NewUnstartedServer(nil)
	//ts.EnableHTTP2 = true
	//ts.StartTLS()
	//defer ts.Close()
	//dispatcherBackend, err := NewDispatcherBackend(DefaultHttpAddress, ts.TLS, dispatcherStore)
	//assert.NoError(t, err)
	//go func() {
	//	err := dispatcherBackend.Start()
	//	assert.EqualError(t, err, http.ErrServerClosed.Error())
	//}()
	//defer func() {
	//	err := dispatcherBackend.Stop(context.Background())
	//	assert.NoError(t, err)
	//}()

	commandHandler := NewCommandHandler(dispatcherStore)

	c := &Command{Operation: addOperation, Sec: "p", Ptype: "p", Rules: nil}
	b, err := json.Marshal(c)
	assert.NoError(t, err)

	dispatcherStore.EXPECT().Apply(b).Return(nil)
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(b))
	w := httptest.NewRecorder()
	commandHandler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK, w.Code)

	dispatcherStore.EXPECT().Apply(b).Return(errors.New("error"))
	r = httptest.NewRequest(http.MethodPost, "/", bytes.NewBuffer(b))
	w = httptest.NewRecorder()
	commandHandler.ServeHTTP(w, r)
	assert.Equal(t, http.StatusOK != w.Code, true)
}

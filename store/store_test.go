package store

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/casbin/casbin/v2"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/raft"
	"github.com/nodece/casbin-hraft-dispatcher/command"
	"github.com/nodece/casbin-hraft-dispatcher/store/mocks"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func GetLocalIP() string {
	return "127.0.0.1"
}

func GetTLSConfig() (*tls.Config, error) {
	rootCAPool := x509.NewCertPool()
	rootCA, err := ioutil.ReadFile("../testdata/ca/ca.pem")
	if err != nil {
		return nil, err
	}
	rootCAPool.AppendCertsFromPEM(rootCA)

	cert, err := tls.LoadX509KeyPair("../testdata/ca/peer.pem", "../testdata/ca/peer-key.pem")
	if err != nil {
		return nil, err
	}

	config := &tls.Config{
		RootCAs:      rootCAPool,
		ClientCAs:    rootCAPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		Certificates: []tls.Certificate{cert},
	}

	return config, nil
}

func TestStore_SingleNode(t *testing.T) {
	ctl := gomock.NewController(t)
	defer ctl.Finish()

	enforcer := mocks.NewMockIDistributedEnforcer(ctl)
	raftID := "node-leader"

	raftAddress := GetLocalIP() + ":6790"

	store, err := newStore(enforcer, raftID, raftAddress, true)
	assert.NoError(t, err)
	defer store.Stop()
	defer os.RemoveAll(store.DataDir())

	err = store.WaitLeader()
	assert.NoError(t, err)

	Convey("TestStore_SingleNode", t, func() {
		Convey("AddPolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.AddPoliciesRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			enforcer.EXPECT().AddPoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			err := store.AddPolicies(request)
			So(err, ShouldBeNil)
		})

		Convey("RemovePolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.RemovePoliciesRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			enforcer.EXPECT().RemovePoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			err := store.RemovePolicies(request)
			So(err, ShouldBeNil)
		})

		Convey("RemoveFilteredPolicy()", func() {
			sec := "p"
			pType := "p"
			fieldIndex := 0
			fieldValues := []string{"role:admin"}
			effected := [][]string{{"role:admin", "/", "*"}}

			request := &command.RemoveFilteredPolicyRequest{
				Sec:         sec,
				PType:       pType,
				FieldIndex:  int32(fieldIndex),
				FieldValues: fieldValues,
			}

			enforcer.EXPECT().RemoveFilteredPolicySelf(nil, sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			err := store.RemoveFilteredPolicy(request)
			So(err, ShouldBeNil)
		})

		Convey("UpdatePolicy()", func() {
			sec := "p"
			pType := "p"
			oldRule := []string{"role:admin", "/", "*"}
			newRule := []string{"role:admin", "/admin", "*"}

			request := &command.UpdatePolicyRequest{
				Sec:     sec,
				PType:   pType,
				OldRule: oldRule,
				NewRule: newRule,
			}

			enforcer.EXPECT().UpdatePolicySelf(nil, sec, pType, oldRule, newRule).Return(true, nil)
			err := store.UpdatePolicy(request)
			So(err, ShouldBeNil)
		})

		Convey("ClearPolicy()", func() {
			enforcer.EXPECT().ClearPolicySelf(nil).Return(nil)
			err := store.ClearPolicy()
			So(err, ShouldBeNil)
		})

		Convey("ID()", func() {
			assert.Equal(t, raftID, store.ID())
			So(store.ID(), ShouldEqual, raftID)
		})

		Convey("Address()", func() {
			So(store.Address(), ShouldEqual, raftAddress)
		})

		Convey("Leader()", func() {
			isLeader, leaderAddress := store.Leader()

			So(isLeader, ShouldBeTrue)
			So(leaderAddress, ShouldEqual, raftAddress)
		})

		Convey("IsInitializedCluster()", func() {
			ok := store.IsInitializedCluster()
			So(ok, ShouldBeTrue)
		})
	})
}

func TestStore_MultipleNode(t *testing.T) {
	// mock leader enforcer
	leaderCtl := gomock.NewController(t)
	defer leaderCtl.Finish()
	leaderEnforcer := mocks.NewMockIDistributedEnforcer(leaderCtl)

	// mock follower
	followerCtl := gomock.NewController(t)
	defer followerCtl.Finish()
	followerEnforcer := mocks.NewMockIDistributedEnforcer(followerCtl)

	localIP := GetLocalIP()
	leaderAddress := localIP + ":6790"
	followerAddress := localIP + ":6780"

	leaderID := "node-leader"
	followerID := "node-follower"

	leaderStore, err := newStore(leaderEnforcer, leaderID, leaderAddress, true)
	assert.NoError(t, err)

	err = leaderStore.WaitLeader()
	assert.NoError(t, err)

	followerStore, err := newStore(followerEnforcer, followerID, followerAddress, false)
	assert.NoError(t, err)

	err = leaderStore.JoinNode(followerStore.ID(), followerStore.Address())
	assert.NoError(t, err)

	err = followerStore.WaitLeader()
	assert.NoError(t, err)

	Convey("TestStore_MultipleNode", t, func() {
		Convey("AddPolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.AddPoliciesRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			leaderEnforcer.EXPECT().AddPoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			followerEnforcer.EXPECT().AddPoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			err := leaderStore.AddPolicies(request)
			So(err, ShouldBeNil)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		Convey("RemovePolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.RemovePoliciesRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			leaderEnforcer.EXPECT().RemovePoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			followerEnforcer.EXPECT().RemovePoliciesSelf(nil, sec, pType, originalRules).Return(originalRules, nil)
			err := leaderStore.RemovePolicies(request)
			So(err, ShouldBeNil)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		Convey("RemoveFilteredPolicy()", func() {
			sec := "p"
			pType := "p"
			fieldIndex := 0
			fieldValues := []string{"role:admin"}
			effected := [][]string{{"role:admin", "/", "*"}}

			request := &command.RemoveFilteredPolicyRequest{
				Sec:         sec,
				PType:       pType,
				FieldIndex:  int32(fieldIndex),
				FieldValues: fieldValues,
			}

			leaderEnforcer.EXPECT().RemoveFilteredPolicySelf(nil, sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			followerEnforcer.EXPECT().RemoveFilteredPolicySelf(nil, sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			err := leaderStore.RemoveFilteredPolicy(request)
			So(err, ShouldBeNil)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		Convey("UpdatePolicy()", func() {
			sec := "p"
			pType := "p"
			oldRule := []string{"role:admin", "/", "*"}
			newRule := []string{"role:admin", "/admin", "*"}

			request := &command.UpdatePolicyRequest{
				Sec:     sec,
				PType:   pType,
				OldRule: oldRule,
				NewRule: newRule,
			}

			leaderEnforcer.EXPECT().UpdatePolicySelf(nil, sec, pType, oldRule, newRule).Return(true, nil)
			followerEnforcer.EXPECT().UpdatePolicySelf(nil, sec, pType, oldRule, newRule).Return(true, nil)
			err := leaderStore.UpdatePolicy(request)
			So(err, ShouldBeNil)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		Convey("ClearPolicy()", func() {
			leaderEnforcer.EXPECT().ClearPolicySelf(nil).Return(nil)
			followerEnforcer.EXPECT().ClearPolicySelf(nil).Return(nil)
			err := leaderStore.ClearPolicy()
			So(err, ShouldBeNil)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		Convey("ID()", func() {
			So(leaderStore.ID(), ShouldEqual, leaderID)
			So(followerStore.ID(), ShouldEqual, followerID)
		})

		Convey("Address()", func() {
			So(leaderStore.Address(), ShouldEqual, leaderAddress)
			So(followerStore.Address(), ShouldEqual, followerAddress)
		})

		Convey("Leader()", func() {
			isLeader, address := leaderStore.Leader()
			So(isLeader, ShouldBeTrue)
			So(address, ShouldEqual, leaderAddress)

			isLeader, address = followerStore.Leader()
			So(isLeader, ShouldBeFalse)
			So(address, ShouldEqual, leaderAddress)
		})

		Convey("RemoveNode()", func() {
			err := leaderStore.RemoveNode(followerAddress)
			So(err, ShouldBeNil)
		})

		Convey("IsInitializedCluster()", func() {
			ok := leaderStore.IsInitializedCluster()
			So(ok, ShouldBeTrue)

			ok = followerStore.IsInitializedCluster()
			So(ok, ShouldBeTrue)
		})
	})
}

func newStore(enforcer casbin.IDistributedEnforcer, id string, address string, enableBootstrap bool) (*Store, error) {
	dir, err := ioutil.TempDir("", "casbin-hraft-")
	if err != nil {
		return nil, err
	}

	tlsConfig, err := GetTLSConfig()
	if err != nil {
		return nil, err
	}

	streamLayer, err := NewTCPStreamLayer(address, tlsConfig)
	if err != nil {
		return nil, err
	}

	store, err := NewStore(&Config{
		ID:  id,
		Dir: dir,
		NetworkTransportConfig: &raft.NetworkTransportConfig{
			Logger:  nil,
			Stream:  streamLayer,
			MaxPool: 5,
			Timeout: 10 * time.Second,
		},
		Enforcer: enforcer,
	})
	if err != nil {
		return nil, err
	}

	err = store.Start(enableBootstrap)
	if err != nil {
		return nil, err
	}

	return store, nil
}

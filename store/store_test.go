package store

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/hashicorp/raft"
	"github.com/nodece/casbin-hraft-dispatcher/command"
	"github.com/nodece/casbin-hraft-dispatcher/store/mocks"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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

func TestStore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Store Suite")
}

func newStore(id string, address string, enableBootstrap bool) (*gomock.Controller, *mocks.MockIDistributedEnforcer, *Store, string) {
	ctl := gomock.NewController(GinkgoT())
	enforcer := mocks.NewMockIDistributedEnforcer(ctl)

	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(GinkgoT(), err)

	tlsConfig, err := GetTLSConfig()
	assert.NoError(GinkgoT(), err)

	streamLayer, err := NewTCPStreamLayer(address, tlsConfig)
	assert.NoError(GinkgoT(), err)

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
	assert.NoError(GinkgoT(), err)

	err = store.Start(enableBootstrap)
	assert.NoError(GinkgoT(), err)

	return ctl, enforcer, store, dir
}

var _ = Describe("test store", func() {
	Describe("SingleNode", func() {
		var ctl *gomock.Controller
		var enforcer *mocks.MockIDistributedEnforcer
		var store *Store
		var workDir string
		var raftAddress string
		raftID := "node-leader"

		BeforeEach(func() {
			localIP := GetLocalIP()
			raftAddress = localIP + ":6791"

			ctl, enforcer, store, workDir = newStore(raftID, raftAddress, true)
			err := store.WaitLeader()
			assert.NoError(GinkgoT(), err)
		})

		AfterEach(func() {
			ctl.Finish()
			store.Stop()
			os.RemoveAll(workDir)
		})

		It("AddPolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.AddPolicyRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			enforcer.EXPECT().AddPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			err := store.AddPolicy(request)
			assert.NoError(GinkgoT(), err)
		})

		It("RemovePolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.RemovePolicyRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			enforcer.EXPECT().RemovePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			err := store.RemovePolicy(request)
			assert.NoError(GinkgoT(), err)
		})

		It("RemoveFilteredPolicy()", func() {
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

			enforcer.EXPECT().RemoveFilteredPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			err := store.RemoveFilteredPolicy(request)
			assert.NoError(GinkgoT(), err)
		})

		It("UpdatePolicy()", func() {
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

			enforcer.EXPECT().UpdatePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, oldRule, newRule).Return(true, nil)
			err := store.UpdatePolicy(request)
			assert.NoError(GinkgoT(), err)
		})

		It("ClearPolicy()", func() {
			enforcer.EXPECT().ClearPolicySelf(gomock.AssignableToTypeOf(ShouldPersist)).Return(nil)
			err := store.ClearPolicy()
			assert.NoError(GinkgoT(), err)
		})

		It("ID()", func() {
			assert.Equal(GinkgoT(), raftID, store.ID())
		})

		It("Address()", func() {
			assert.Equal(GinkgoT(), raftAddress, store.Address())
		})

		It("Leader()", func() {
			isLeader, leaderAddress := store.Leader()

			assert.Equal(GinkgoT(), true, isLeader)
			assert.Equal(GinkgoT(), raftAddress, leaderAddress)
		})

		It("IsInitializedCluster()", func() {
			ok := store.IsInitializedCluster()
			assert.Equal(GinkgoT(), true, ok)
		})
	})

	Describe("MultipleNode", func() {
		var leaderCtl *gomock.Controller
		var leaderEnforcer *mocks.MockIDistributedEnforcer
		var leaderStore *Store
		var leaderWorkDir string
		var leaderAddress string
		leaderID := "node-leader"

		var followerCtl *gomock.Controller
		var followerEnforcer *mocks.MockIDistributedEnforcer
		var followerStore *Store
		var followerWorkDir string
		var followerAddress string
		followerID := "node-follower"

		BeforeEach(func() {
			localIP := GetLocalIP()
			leaderAddress = localIP + ":6790"
			leaderCtl, leaderEnforcer, leaderStore, leaderWorkDir = newStore(leaderID, leaderAddress, true)
			err := leaderStore.WaitLeader()
			assert.NoError(GinkgoT(), err)

			followerAddress = localIP + ":6791"
			followerCtl, followerEnforcer, followerStore, followerWorkDir = newStore(followerID, followerAddress, false)
			err = leaderStore.JoinNode(followerStore.ID(), followerStore.Address())
			assert.NoError(GinkgoT(), err)
			err = followerStore.WaitLeader()
			assert.NoError(GinkgoT(), err)
		})

		AfterEach(func() {
			leaderCtl.Finish()
			leaderStore.Stop()
			os.RemoveAll(leaderWorkDir)

			followerCtl.Finish()
			followerStore.Stop()
			os.RemoveAll(followerWorkDir)
		})

		It("AddPolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.AddPolicyRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			leaderEnforcer.EXPECT().AddPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			followerEnforcer.EXPECT().AddPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			err := leaderStore.AddPolicy(request)
			assert.NoError(GinkgoT(), err)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		It("RemovePolicy()", func() {
			sec := "p"
			pType := "p"
			originalRules := [][]string{{"role:admin", "/", "*"}}
			var rules []*command.StringArray
			for _, rule := range originalRules {
				rules = append(rules, &command.StringArray{Items: rule})
			}

			request := &command.RemovePolicyRequest{
				Sec:   sec,
				PType: pType,
				Rules: rules,
			}

			leaderEnforcer.EXPECT().RemovePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			followerEnforcer.EXPECT().RemovePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, originalRules).Return(originalRules, nil)
			err := leaderStore.RemovePolicy(request)
			assert.NoError(GinkgoT(), err)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		It("RemoveFilteredPolicy()", func() {
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

			leaderEnforcer.EXPECT().RemoveFilteredPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			followerEnforcer.EXPECT().RemoveFilteredPolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, fieldIndex, fieldValues).Return(effected, nil)
			err := leaderStore.RemoveFilteredPolicy(request)
			assert.NoError(GinkgoT(), err)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		It("UpdatePolicy()", func() {
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

			leaderEnforcer.EXPECT().UpdatePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, oldRule, newRule).Return(true, nil)
			followerEnforcer.EXPECT().UpdatePolicySelf(gomock.AssignableToTypeOf(ShouldPersist), sec, pType, oldRule, newRule).Return(true, nil)
			err := leaderStore.UpdatePolicy(request)
			assert.NoError(GinkgoT(), err)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		It("ClearPolicy()", func() {
			leaderEnforcer.EXPECT().ClearPolicySelf(gomock.AssignableToTypeOf(ShouldPersist)).Return(nil)
			followerEnforcer.EXPECT().ClearPolicySelf(gomock.AssignableToTypeOf(ShouldPersist)).Return(nil)
			err := leaderStore.ClearPolicy()
			assert.NoError(GinkgoT(), err)

			// Waiting for synchronization data to follow node.
			<-time.After(2 * time.Second)
		})

		It("ID()", func() {
			assert.Equal(GinkgoT(), leaderID, leaderStore.ID())
			assert.Equal(GinkgoT(), followerID, followerStore.ID())
		})

		It("Address()", func() {
			assert.Equal(GinkgoT(), leaderAddress, leaderStore.Address())
			assert.Equal(GinkgoT(), followerAddress, followerStore.Address())
		})

		It("Leader()", func() {
			isLeader, address := leaderStore.Leader()
			assert.Equal(GinkgoT(), true, isLeader)
			assert.Equal(GinkgoT(), leaderAddress, address)

			isLeader, address = followerStore.Leader()
			assert.Equal(GinkgoT(), false, isLeader)
			assert.Equal(GinkgoT(), leaderAddress, address)
		})

		It("RemoveNode()", func() {
			err := leaderStore.RemoveNode(followerAddress)
			assert.NoError(GinkgoT(), err)
		})

		It("IsInitializedCluster()", func() {
			ok := leaderStore.IsInitializedCluster()
			assert.Equal(GinkgoT(), true, ok)

			ok = followerStore.IsInitializedCluster()
			assert.Equal(GinkgoT(), true, ok)
		})
	})
})

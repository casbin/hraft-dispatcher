package store

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/casbin/casbin/v2"

	"github.com/casbin/hraft-dispatcher/command"
	"github.com/casbin/hraft-dispatcher/store/mocks"
	"github.com/golang/mock/gomock"
	"github.com/hashicorp/raft"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func GetLocalIP() string {
	return "127.0.0.1"
}

var rootCA = []byte(`
-----BEGIN CERTIFICATE-----
MIIDaDCCAlCgAwIBAgIUcpVt/SzT9IPq/HWZOTP8WXB//5owDQYJKoZIhvcNAQEL
BQAwTDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRYwFAYDVQQHEw1TYW4gRnJh
bmNpc2NvMRgwFgYDVQQDEw9ocmFmdGRpc3BhdGNoZXIwHhcNMjEwMjE1MTUwNDAw
WhcNMjYwMjE0MTUwNDAwWjBMMQswCQYDVQQGEwJVUzELMAkGA1UECBMCQ0ExFjAU
BgNVBAcTDVNhbiBGcmFuY2lzY28xGDAWBgNVBAMTD2hyYWZ0ZGlzcGF0Y2hlcjCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAL0pK4CAtBjs1IM3savAJs5p
IJdWQRi19SE1U6ypy/UMQJRrhdSm9J1RuYNQfELl0RkWaNDsi0L6G0kbLMs5kkFj
7ySpPeCTE6UhGOCWXwWQ/eqJNgeJWMwLiErX+fIT/SE3xuu4e5xJkF1gVAGpGp6Z
Uhj2Ilc+xRexIhbv6kgbkh+cG1M0qxGWsb8QQ7ohz2yHMsEkG/lsHxehME0K7Y1j
EotLuAEC/UHSzYqA643V2a7rFSmM+sQMzBJ6H2ENsglpVCfLyoP5iTbhnOnHwqUS
sMk9kfedo0mjLjFmCW81SUjY3Y/h8H2kPPmmy8aUT9H120EGoIre7FZyiZBu5hEC
AwEAAaNCMEAwDgYDVR0PAQH/BAQDAgEGMA8GA1UdEwEB/wQFMAMBAf8wHQYDVR0O
BBYEFMvdedWPdZrkG0ZNAV8fIX+F6Dg2MA0GCSqGSIb3DQEBCwUAA4IBAQCmkvnH
XKURz70aU5DXeuR4/8kvEk/Wv25154rZfaa7g7uEfSITvgj+7e65HJW53BT7Xrpj
xfALJriaONcf4xu+ov1rdMdojpYnRwNC4B5tiNUK+IEkr/9iyOk7Tu3fY1hwbzLz
DjfUP+9aOdzBW4xapVpM+YlvIR525rXl+P062SEm9pUyWvDFAb3tKHX9W6Hjzm5w
50qShl7ncwwtTYUylVUZcS1xOh0vF4WPtGWbuN0jfQjLFDeyG2l78jcl/ngsIiyT
KTExqCSHJ/6FT9zQnYhXEKU4BLIzqeS8Kfnd7h+3URYZNs5GlfujhLflaR6/Xxp1
/P7UwWnW1xZf+iAP
-----END CERTIFICATE-----
`)

var peerCA = []byte(`
-----BEGIN CERTIFICATE-----
MIIDlzCCAn+gAwIBAgIUUVw2A/52WC/efPDTngQd47EJWqgwDQYJKoZIhvcNAQEL
BQAwTDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkNBMRYwFAYDVQQHEw1TYW4gRnJh
bmNpc2NvMRgwFgYDVQQDEw9ocmFmdGRpc3BhdGNoZXIwIBcNMjEwMjE1MTUwNDAw
WhgPMjEyMTAxMjIxNTA0MDBaMEwxCzAJBgNVBAYTAlVTMQswCQYDVQQIEwJDQTEW
MBQGA1UEBxMNU2FuIEZyYW5jaXNjbzEYMBYGA1UEAxMPaHJhZnRkaXNwYXRjaGVy
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAxHaJUQTh3eM8ztbC9Vnq
9NtqaPJ7+GcymOzPjDBVLGuBzbTY2Hd7D6gV0Hih9QW9yneN/jI9u4e3CHfln3aQ
0t7rVvu3CAEx+34M49nWIZNebKU+j3YSmCkxl/I2fD3x/vSvTy41Ey4DHVjbw35c
DE/vg6eU+nnkX96tbrVZgUnZfrSaI8xmdO64DLqOCNUcl0LM8zXAgH+bIkKM6y4U
6AAEsKsZ5CNpMw0nopCnuQZu0NFFNM9qgUdbJLSHOqGbFLiIGX+PNis9dVggZX3U
dzbyFHtAywM53S5Hq0Q2S2dYQHLhSFkWgwCZEDP+Oi3j46vXVHzNZgJng+wHNiCB
zwIDAQABo28wbTAOBgNVHQ8BAf8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEG
CCsGAQUFBwMCMAwGA1UdEwEB/wQCMAAwHQYDVR0OBBYEFHPb6d7J8ai2eYyX+0aF
MzKPCHktMA8GA1UdEQQIMAaHBH8AAAEwDQYJKoZIhvcNAQELBQADggEBAHtgS5pR
WL72X6p8+levu0lumia5fOAmbDylarcLn77qFbl0xK6ebDBQx6HXk1ZOzVB+DcJj
z/BJ6gdoT9211C0VeT4VGE8rh28IRL9mhOXv1/sXuJo8qPH+VwR95phFD1+wcwBG
f6GJde1qncnc79Sh+KfcYKpmbNxBLZA96LKRrN+kaytQDeB0EFTZBMc9XhdQsm/8
3QMkGh7Q1wJNzLmw8nUDmz8gtucQ00pwDe4PKSmrsFSJq62aMYhNeqItEGT3WV0e
EzHG/uXVRwqgOepAA3p4neKaAH1G+oGWEzcI0EPPB3uCJzGLJTEU6QX0ISpyN8sT
ihn/9JzaGivK+fk=
-----END CERTIFICATE-----
`)

var peerKey = []byte(`
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAxHaJUQTh3eM8ztbC9Vnq9NtqaPJ7+GcymOzPjDBVLGuBzbTY
2Hd7D6gV0Hih9QW9yneN/jI9u4e3CHfln3aQ0t7rVvu3CAEx+34M49nWIZNebKU+
j3YSmCkxl/I2fD3x/vSvTy41Ey4DHVjbw35cDE/vg6eU+nnkX96tbrVZgUnZfrSa
I8xmdO64DLqOCNUcl0LM8zXAgH+bIkKM6y4U6AAEsKsZ5CNpMw0nopCnuQZu0NFF
NM9qgUdbJLSHOqGbFLiIGX+PNis9dVggZX3UdzbyFHtAywM53S5Hq0Q2S2dYQHLh
SFkWgwCZEDP+Oi3j46vXVHzNZgJng+wHNiCBzwIDAQABAoIBAEOn8oflH4dTHvi3
+rGVgpVKDm4Pu2OC3mjNfHfxmRNP/oaBlf+NveJZZxHAyT1g+cgEvfBhCuNOzFht
ObVdlmgX/oGY86IdD0JlWTkKJnSvlF/j1BSBe8vMu9hwwBSvHGxJhSnGZt6xBL+R
fzTmifpveLMk/ef4HA5r19v9NdKQqhxF3SfY2SPCcdwrtsbTxwKvKXaIKIzQ0j5o
HGQPwiCLWYXWIBfwqpNB0FWpnBhSIUi3U/fYrfdRWsBCaCKaJFLrknTYk2jPTYHd
kXUNBwUyiRpD+c8MCVXhVrd6lg8MYK4qKs2k5VtfDtNM5uxegD2elEvbB9XDbdL9
pXBP0okCgYEA8h51ocpteiyv/kZfHnRgHgIB9zGOBHXK2oIir843jezdTnLjLKqa
GY8/6Rp6Djmbnw8q5wPtP1OgT2Zlvt93fjd1o2zZsA5LfBo07T4BQ0hIPQLSBkl0
GfKKnPJBgKlhDSiopr3xDlPkx8BX9PP1Sf/G5ewYC/QpWW55NP4s02UCgYEAz7n9
9WRRFa1fXSEvcsXCOxHpKc+/XG6ZfptPnrmHVGv+MrHNE8GTWupoTr8l1gNsp1IH
I3pG0THEMlNGApF4u1v2tZ5yNnsoVeP9Zrwy9i3E56weIOqBlpCxbjW8FPmTgVzt
Xp/2IxBaZQn9YdCLSN2oX1Pqb7K3J84PzaXW/yMCgYBnVuDWQVQgxVoIqXiHwxwT
MsAsBZacCLqgMNMlPlsv1F1Q0nBr7BUBu8aHc6mM0MG/TfX9zAtC8CqIOShMI40Y
7grjyd3P6woE2hqk98YKNZu/jqidzlQjjwXinvOeOq0VtLjnEkME3oHTUCE6h7W1
89ms4OwSjg/n/+Lz31i6kQKBgQCXM/M/o/3BoalAyN8Y1ApFpQvre2T3iyn/ll2m
U7XGJbWqgPGd59Gy4915NHn+BhAY2wSHNoJF08vUNflH9UvEVXSHTwYj0hHSM1pI
ZcVSnI4vdIGZxBj/1+LPLh3xxpkwGMxPjHBFpamm0la11G8OYwokGZkUJSpctwmZ
z5VnsQKBgHdLPABKFGfxwsLFEr6vnwS3CmUcwbijDy7YGW//HOSk+sqsPuJclaSt
BIBwORizBRWQd+C5aDa3lu9pkBjlZ+b57RnYq+bzQbNDHUt3c7BtcCujceD0uVXz
/hug3iKji4mRJKDqV+5lYNG+Q6T3vZ5c7NbMo2LmeNXu/1aOxk4z
-----END RSA PRIVATE KEY-----
`)

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

	store, err := newStore("", enforcer, raftID, raftAddress, true, false)
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

	leaderStore, err := newStore("", leaderEnforcer, leaderID, leaderAddress, true, false)
	assert.NoError(t, err)
	defer leaderStore.Stop()

	err = leaderStore.WaitLeader()
	assert.NoError(t, err)

	followerStore, err := newStore("", followerEnforcer, followerID, followerAddress, false, false)
	assert.NoError(t, err)
	defer followerStore.Stop()

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

func TestStore_MigrateFileSnapshot(t *testing.T) {
	dir, err := ioutil.TempDir("", "casbin-hraft-")
	assert.NoError(t, err)

	fileSnapshot, err := raft.NewFileSnapshotStore(dir, retainSnapshotCount, os.Stderr)
	assert.NoError(t, err)
	_, trans := raft.NewInmemTransport(raft.NewInmemAddr())

	ctl := gomock.NewController(t)
	defer ctl.Finish()

	enforcer := mocks.NewMockIDistributedEnforcer(ctl)
	p, err := NewPolicyOperator(zap.NewExample(), dir, enforcer)
	assert.NoError(t, err)

	enforcer.EXPECT().AddPoliciesSelf(nil, "p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}).Return([][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}}, nil)
	err = p.AddPolicies("p", "p", [][]string{{"role:admin", "/", "*"}, {"role:user", "/", "GET"}})
	assert.NoError(t, err)
	b, err := p.Backup()
	assert.NoError(t, err)
	size := len(b)
	p.db.Close()

	for i := 0; i < 3; i++ {
		var sink raft.SnapshotSink
		sink, err = fileSnapshot.Create(1, uint64(i), 3, raft.Configuration{}, 0, trans)
		assert.NoError(t, err)
		_, err := sink.Write(b)
		assert.NoError(t, err)
		err = sink.Close()
		assert.NoError(t, err)
	}

	snaps, err := fileSnapshot.List()
	assert.NoError(t, err)
	assert.Equal(t, retainSnapshotCount, len(snaps))

	raftID := "node-leader"
	raftAddress := GetLocalIP() + ":6790"

	enforcer.EXPECT().ClearPolicySelf(nil)
	enforcer.EXPECT().AddPoliciesSelf(nil, "p", "p", [][]string{{"role:admin", "/", "*"}})
	enforcer.EXPECT().AddPoliciesSelf(nil, "p", "p", [][]string{{"role:user", "/", "GET"}})

	// Create store with bbolt snapshots
	store, err := newStore(dir, enforcer, raftID, raftAddress, true, true)
	assert.NoError(t, err)
	defer store.Stop()
	defer os.RemoveAll(store.DataDir())

	// Make sure file snapshot directory does not exist
	const snapPath = "snapshots"
	snapDir := filepath.Join(dir, snapPath)
	_, err = os.Stat(snapDir)
	assert.Error(t, err)

	// Make sure the data is consistent
	snaps, err = store.snapshotStore.List()
	assert.NoError(t, err)
	assert.Equal(t, retainSnapshotCount, len(snaps))

	for k, v := range snaps {
		assert.Equal(t, uint64(2-k), v.Index)
		assert.Equal(t, uint64(3), v.Term)
		assert.Equal(t, true, reflect.DeepEqual(v.Configuration, raft.Configuration{}))
		assert.Equal(t, uint64(0), v.ConfigurationIndex)
		assert.Equal(t, int64(size), v.Size)
	}
}

func newStore(dataDir string, enforcer casbin.IDistributedEnforcer, id string, address string, enableBootstrap, useBoltSnapshot bool) (*Store, error) {
	dir := dataDir
	if dir == "" {
		var err error
		dir, err = ioutil.TempDir("", "casbin-hraft-")
		if err != nil {
			return nil, err
		}
	}

	tlsConfig, err := GetTLSConfig()
	if err != nil {
		return nil, err
	}

	ln, err := tls.Listen("tcp", address, tlsConfig)
	if err != nil {
		return nil, err
	}

	streamLayer, err := NewTCPStreamLayer(ln, tlsConfig)
	if err != nil {
		return nil, err
	}

	store, err := NewStore(zap.NewExample(), &Config{
		ID:  id,
		Dir: dir,
		NetworkTransportConfig: &raft.NetworkTransportConfig{
			Logger:  nil,
			Stream:  streamLayer,
			MaxPool: 5,
			Timeout: 10 * time.Second,
		},
		Enforcer:        enforcer,
		UseBoltSnapshot: useBoltSnapshot,
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

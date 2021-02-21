package hraftdispatcher

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func ToGenericArray(arr []string) []interface{} {
	s := make([]interface{}, len(arr))
	for i, v := range arr {
		s[i] = v
	}
	return s
}

func TestDispatcher(t *testing.T) {
	var leaderEnforcer casbin.IDistributedEnforcer
	var leaderDispatcher *HRaftDispatcher
	var followerEnforcer casbin.IDistributedEnforcer
	var followerDispatcher *HRaftDispatcher

	dataDir, err := ioutil.TempDir("", "casbin-hraft-dispatcher-")
	assert.NoError(t, err)
	defer os.RemoveAll(dataDir)

	leaderRaftAddress := "127.0.0.1:6780"
	leaderEnforcer, leaderDispatcher, err = newNode(dataDir, leaderRaftAddress, "")
	assert.NoError(t, err)
	defer leaderDispatcher.Shutdown()

	followerRaftAddress := "127.0.0.1:6790"
	followerEnforcer, followerDispatcher, err = newNode(dataDir, followerRaftAddress, leaderRaftAddress)
	assert.NoError(t, err)
	defer followerDispatcher.Shutdown()

	Convey("test dispatcher", t, func() {
		Convey("test in leader node", func() {
			Convey("test AddPolicy()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range rules {
					_, err := leaderEnforcer.AddPolicy(rule)
					So(err, ShouldBeNil)
				}

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test UpdatePolicy()", func() {
				oldRule := []string{"role:admin", "/", "GET"}
				newRule := []string{"role:admin", "/", "DELETE"}
				_, err := leaderEnforcer.UpdatePolicy(oldRule, newRule)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrue := [][]string{
					{"role:admin", "/", "DELETE"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range expectedTrue {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test RemovePolicy()", func() {
				_, err := leaderEnforcer.RemovePolicy("role:admin", "/", "POST")
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrueRules := [][]string{
					{"role:admin", "/", "DELETE"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range expectedTrueRules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}

				expectedFalseRules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
				}
				for _, rule := range expectedFalseRules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test ClearPolicy()", func() {
				leaderEnforcer.ClearPolicy()

				<-time.After(time.Second * 3)

				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test AddPolicies()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				_, err := leaderEnforcer.AddPolicies(rules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test UpdatePolicies()", func() {
				oldRules := [][]string{{"role:admin", "/", "GET"}, {"role:admin", "/", "POST"}}
				newRules := [][]string{{"role:admin", "/admin", "GET"}, {"role:admin", "/admin", "POST"}}
				_, err := leaderEnforcer.UpdatePolicies(oldRules, newRules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrue := [][]string{
					{"role:admin", "/admin", "GET"},
					{"role:admin", "/admin", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				for _, rule := range expectedTrue {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}

				expectedFalse := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
				}

				for _, rule := range expectedFalse {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test RemovePolicies()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "GET"},
					{"role:admin", "/admin", "GET"},
					{"role:admin", "/admin", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				_, err := leaderEnforcer.RemovePolicies(rules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("cleanup test", func() {
				leaderEnforcer.ClearPolicy()

				<-time.After(time.Second * 3)
			})
		})

		Convey("test in follower node", func() {
			Convey("test AddPolicy()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range rules {
					_, err := followerEnforcer.AddPolicy(rule)
					So(err, ShouldBeNil)
				}

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test UpdatePolicy()", func() {
				oldRule := []string{"role:admin", "/", "GET"}
				newRule := []string{"role:admin", "/", "DELETE"}
				_, err := followerEnforcer.UpdatePolicy(oldRule, newRule)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrue := [][]string{
					{"role:admin", "/", "DELETE"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range expectedTrue {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test RemovePolicy()", func() {
				_, err := followerEnforcer.RemovePolicy("role:admin", "/", "POST")
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrueRules := [][]string{
					{"role:admin", "/", "DELETE"},
					{"role:admin", "/", "PUT"},
				}
				for _, rule := range expectedTrueRules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}

				expectedFalseRules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
				}
				for _, rule := range expectedFalseRules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test ClearPolicy()", func() {
				followerEnforcer.ClearPolicy()

				<-time.After(3 * time.Second)

				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test AddPolicies()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				_, err := followerEnforcer.AddPolicies(rules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}
			})

			Convey("test UpdatePolicies()", func() {
				oldRules := [][]string{{"role:admin", "/", "GET"}, {"role:admin", "/", "POST"}}
				newRules := [][]string{{"role:admin", "/admin", "GET"}, {"role:admin", "/admin", "POST"}}
				_, err := followerEnforcer.UpdatePolicies(oldRules, newRules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				expectedTrue := [][]string{
					{"role:admin", "/admin", "GET"},
					{"role:admin", "/admin", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				for _, rule := range expectedTrue {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				}

				expectedFalse := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "POST"},
				}

				for _, rule := range expectedFalse {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("test RemovePolicies()", func() {
				rules := [][]string{
					{"role:admin", "/", "GET"},
					{"role:admin", "/", "GET"},
					{"role:admin", "/admin", "GET"},
					{"role:admin", "/admin", "POST"},
					{"role:admin", "/", "PUT"},
					{"role:admin", "/", "DELETE"},
				}

				_, err := followerEnforcer.RemovePolicies(rules)
				So(err, ShouldBeNil)

				<-time.After(3 * time.Second)

				for _, rule := range rules {
					ok, err := leaderEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)

					ok, err = followerEnforcer.Enforce(ToGenericArray(rule)...)
					So(err, ShouldBeNil)
					So(ok, ShouldBeFalse)
				}
			})

			Convey("cleanup test", func() {
				leaderEnforcer.ClearPolicy()
				<-time.After(time.Second * 3)
			})
		})
	})
}

func getTLSConfig() (*tls.Config, error) {
	rootCAPool := x509.NewCertPool()
	rootCA, err := ioutil.ReadFile("./testdata/ca/ca.pem")
	if err != nil {
		return nil, err
	}
	rootCAPool.AppendCertsFromPEM(rootCA)

	cert, err := tls.LoadX509KeyPair("./testdata/ca/peer.pem", "./testdata/ca/peer-key.pem")
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

func newNode(dataDir, raftListenAddress, joinAddress string) (casbin.IDistributedEnforcer, *HRaftDispatcher, error) {
	var modelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act
`
	m, err := model.NewModelFromString(modelText)
	if err != nil {
		return nil, nil, err
	}

	e, err := casbin.NewDistributedEnforcer(m)
	if err != nil {
		return nil, nil, err
	}

	tlsConfig, err := getTLSConfig()
	if err != nil {
		return nil, nil, err
	}

	dir, err := ioutil.TempDir(dataDir, "data-")
	if err != nil {
		return nil, nil, err
	}

	dispatcher, err := NewHRaftDispatcher(&Config{
		Enforcer:          e,
		JoinAddress:       joinAddress,
		RaftListenAddress: raftListenAddress,
		TLSConfig:         tlsConfig,
		DataDir:           dir,
	})
	if err != nil {
		return nil, nil, err
	}

	e.SetDispatcher(dispatcher)

	return e, dispatcher, nil
}

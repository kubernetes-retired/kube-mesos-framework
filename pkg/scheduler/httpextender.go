/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"fmt"
	"net/http"

	"github.com/golang/glog"
)

func (e *HTTPExtender) Filter(w http.ResponseWriter, req *http.Request) {
	e.Framework.Filter(nil, nil)
}

func (e *HTTPExtender) Bind(w http.ResponseWriter, req *http.Request) {
	e.Framework.Bind(nil, "")
}

type HTTPExtender struct {
	Address string
	Port    int32

	Framework Framework
}

func NewHTTPExtender(framework Framework, address string, port int32) *HTTPExtender {
	httpExtender := &HTTPExtender{
		Framework: framework,
		Address:   address,
		Port:      port,
	}

	http.HandleFunc("/filter", httpExtender.Filter)
	http.HandleFunc("/bind", httpExtender.Bind)

	return httpExtender
}

func (e *HTTPExtender) Run() {
	address := fmt.Sprintf("%s:%d", e.Address, e.Port)

	glog.Infof("Start HTTP extender for k8s-scheduler at %s", address)

	glog.Fatal(http.ListenAndServe(address, nil))
}

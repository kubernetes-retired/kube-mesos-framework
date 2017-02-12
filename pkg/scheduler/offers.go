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
	"sync"

	"github.com/golang/glog"
	"github.com/mesos/mesos-go/mesosproto"
)

type Offers struct {
	sync.Mutex
	offers map[string]*mesosproto.Offer
}

func (o *Offers) AddAll(offers []*mesosproto.Offer) {
	o.Lock()
	defer o.Unlock()

	for _, offer := range offers {
		oid := offer.Id.GetValue()
		if _, found := o.offers[oid]; found {
			glog.Errorf("Found another offer in the cache: %v", oid)
		}

		o.offers[oid] = offer
	}
}

func (o *Offers) Delete(id *mesosproto.OfferID) {
	o.Lock()
	defer o.Unlock()

	if _, found := o.offers[id.GetValue()]; found {
		delete(o.offers, id.GetValue())
	}
}

func (o *Offers) DeleteBy(f func(*mesosproto.Offer) bool) {
	o.Lock()
	defer o.Unlock()

	for k, v := range o.offers {
		if f(v) {
			delete(o.offers, k)
		}
	}
}

func (o *Offers) Filter(f func(*mesosproto.Offer) bool) []*mesosproto.Offer {
	o.Lock()
	o.Unlock()

	var offers []*mesosproto.Offer
	for _, v := range o.offers {
		if f(v) {
			offers = append(offers, v)
		}
	}

	return offers
}

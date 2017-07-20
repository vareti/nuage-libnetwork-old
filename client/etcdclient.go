package client

import (
	"context"
	log "github.com/Sirupsen/logrus"
	"github.com/coreos/etcd/client"
	nuageApi "github.com/nuagenetworks/nuage-libnetwork/api"
	nuageConfig "github.com/nuagenetworks/nuage-libnetwork/config"
	"time"
)

const (
	nuageEtcdPrefix = "/nuage/v0/networks/"
	requestTimeout  = 1 * time.Second
)

// NuageEtcdClient client structure that holds etcd config information
type NuageEtcdClient struct {
	etcdServerURL string
	kapi          client.KeysAPI
	etcdChannel   chan *nuageApi.EtcdEvent
	stop          chan bool
}

// NewNuageEtcdClient Initialiezes a new etcd client
func NewNuageEtcdClient(config *nuageConfig.NuageLibNetworkConfig, channels *nuageApi.NuageLibNetworkChannels) (*NuageEtcdClient, error) {
	newEtcdClient := &NuageEtcdClient{}
	newEtcdClient.etcdServerURL = config.EtcdServerURL
	newEtcdClient.etcdChannel = channels.EtcdChannel
	newEtcdClient.stop = make(chan bool)
	c, err := client.New(client.Config{
		Endpoints:               []string{newEtcdClient.etcdServerURL},
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	newEtcdClient.kapi = client.NewKeysAPI(c)
	return newEtcdClient, nil
}

func (nuageetcd *NuageEtcdClient) put(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := nuageetcd.kapi.Set(ctx, nuageEtcdPrefix+"/"+key, value, nil)
	if err != nil {
		log.Errorf("Putting key %s with value %s into etcd failed with error: %v", key, value, err)
		return err
	}
	return nil
}

func (nuageetcd *NuageEtcdClient) get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	resp, err := nuageetcd.kapi.Get(ctx, nuageEtcdPrefix+"/"+key, nil)
	if err != nil {
		log.Errorf("Fetching value for key %s from etcd failed with error: %v", key, err)
		return "", err
	}
	return resp.Node.Value, nil
}

func (nuageetcd *NuageEtcdClient) delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	_, err := nuageetcd.kapi.Delete(ctx, nuageEtcdPrefix+"/"+key, nil)
	if err != nil {
		log.Errorf("Deleting value for key %s from etcd failed with error: %v", key, err)
		return err
	}
	return nil
}

//Start listens for events on etcd channel
func (nuageetcd *NuageEtcdClient) Start() {
	for {
		select {
		case etcdEvent := <-nuageetcd.etcdChannel:
			nuageetcd.handleEtcdEvent(etcdEvent)
		case <-nuageetcd.stop:
			return
		}
	}
}

func (nuageetcd *NuageEtcdClient) handleEtcdEvent(event *nuageApi.EtcdEvent) {
	log.Debugf("Received ETCD event %+v", event)
	eventData := event.EtcdReqObject.(nuageConfig.NuageEtcdParams)
	switch event.EventType {
	case nuageApi.EtcdPutEvent:
		err := nuageetcd.put(eventData.Key, eventData.Value)
		event.EtcdRespObjectChan <- &nuageApi.EtcdRespObject{Error: err}
	case nuageApi.EtcdGetEvent:
		value, err := nuageetcd.get(eventData.Key)
		event.EtcdRespObjectChan <- &nuageApi.EtcdRespObject{EtcdData: value, Error: err}
	case nuageApi.EtcdDeleteEvent:
		err := nuageetcd.delete(eventData.Key)
		event.EtcdRespObjectChan <- &nuageApi.EtcdRespObject{Error: err}
	}
	log.Debugf("Served ETCD event %+v", event)
}

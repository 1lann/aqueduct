package main

import (
	"context"
	"log"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

const aqueductAnnotation = "aqueduct.chuie.io"

type DNSRecord interface {
	Name() string  // @, subdomains, etc.
	Type() string  // A, AAAA, etc.
	Value() string // IP address, etc.
}

type DNSProvider interface {
	ProviderName() string
	GetRecords(rootDomain string) ([]DNSRecord, error)
	CreateRecord(name, typ, value string) error
	ReplaceRecord(original DNSRecord, newValue string) error
	DeleteRecord(record DNSRecord) error
}

const unknownNode = "__aqueduct_internal_unknown_node"

type Aqueduct struct {
	ctx context.Context

	currentState    *AqueductState
	taintEvents     chan struct{}
	informerFactory informers.SharedInformerFactory
	nodesLister     listersv1.NodeLister
	nodesInformer   cache.SharedIndexInformer
	podsLister      listersv1.PodLister
	podsInformer    cache.SharedIndexInformer
	serviceLister   listersv1.ServiceLister
	serviceInformer cache.SharedIndexInformer
	clientset       *kubernetes.Clientset
	providers       map[string]DNSProvider
	ownerName       string

	hasWarnings    bool
	inWarningState atomic.Bool

	lastAchievedDesiredState time.Time
	lastAchievedMutex        sync.Mutex

	ipsToNodes  map[string]string
	ipNodeMutex sync.Mutex
}

func (a *Aqueduct) LastAchievedDesiredState() time.Time {
	a.lastAchievedMutex.Lock()
	defer a.lastAchievedMutex.Unlock()
	return a.lastAchievedDesiredState
}

func (a *Aqueduct) IsInWarningState() bool {
	return a.inWarningState.Load()
}

func (a *Aqueduct) LookupIP(ip string) string {
	a.ipNodeMutex.Lock()
	defer a.ipNodeMutex.Unlock()
	node, found := a.ipsToNodes[ip]
	if !found {
		return unknownNode
	}
	return node
}

func (a *Aqueduct) ResolveHost(nodeName string) (string, error) {
	node, err := a.nodesLister.Get(nodeName)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get node %q", nodeName)
	}

	hostOrIP, found := node.Labels[aqueductAnnotation+"/host"]
	if !found {
		for _, addr := range node.Status.Addresses {
			if addr.Type == corev1.NodeExternalDNS {
				hostOrIP = addr.Address
				break
			}
		}

		if hostOrIP == "" {
			for _, addr := range node.Status.Addresses {
				if addr.Type == corev1.NodeExternalIP {
					hostOrIP = addr.Address
					break
				}
			}
		}
	}

	if hostOrIP == "" {
		return "", errors.Errorf("no host or IP found for node %q", nodeName)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	addrs, err := net.DefaultResolver.LookupHost(ctx, hostOrIP)
	cancel()
	if err != nil {
		return "", errors.Wrapf(err, "failed to resolve %q", hostOrIP)
	}

	if len(addrs) == 0 {
		return "", errors.Errorf("no addresses found for %q", hostOrIP)
	}

	a.ipNodeMutex.Lock()
	a.ipsToNodes[addrs[0]] = nodeName
	a.ipNodeMutex.Unlock()

	return addrs[0], nil
}

// Reconciler is the main loop of the aqueduct controller.
func (a *Aqueduct) Reconciler() {
	taint := func() {
		select {
		case a.taintEvents <- struct{}{}:
		default:
		}
	}

	go func() {
		for {
			select {
			case <-a.ctx.Done():
				return
			case <-time.After(5 * time.Minute):
				if a.LastAchievedDesiredState().Before(time.Now().Add(-5 * time.Minute)) {
					// forcefully taint every 5 minutes
					taint()
				}
			}
		}
	}()

	a.podsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName != "" {
				taint()
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, newPod := oldObj.(*corev1.Pod), newObj.(*corev1.Pod)
			if oldPod.Spec.NodeName != newPod.Spec.NodeName {
				taint()
			}
		},
		DeleteFunc: func(obj interface{}) {
			taint()
		},
	})

	a.serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			taint()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldService, newService := oldObj.(*corev1.Service), newObj.(*corev1.Service)
			if !reflect.DeepEqual(oldService.Spec.Selector, newService.Spec.Selector) ||
				!reflect.DeepEqual(oldService.Annotations, newService.Annotations) {
				taint()
			}
		},
		DeleteFunc: func(obj interface{}) {
			taint()
		},
	})

	go a.informerFactory.Start(a.ctx.Done())

	if !cache.WaitForCacheSync(a.ctx.Done(),
		a.nodesInformer.HasSynced,
		a.podsInformer.HasSynced,
		a.serviceInformer.HasSynced) {
		panic("timed out waiting for caches to sync")
	}

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-a.taintEvents:
		}

		log.Println("reconciling...")

		a.hasWarnings = false
		var plan *Plan
		var desired *AqueductState

		err := a.DiscoverCurrentState()
		if err != nil {
			log.Printf("failed to discover current state: %+v", err)
			goto failed
		}

		log.Printf("current state: %s", a.currentState)

		desired, err = a.GetDesiredState()
		if err != nil {
			log.Printf("failed to get desired state: %+v", err)
			goto failed
		}

		log.Printf("desired state: %s", desired)

		plan = a.GeneratePlan(desired)
		log.Printf("generated plan: %s", plan)

		if plan != nil {
			err = a.ExecutePlan(plan)
			if err != nil {
				log.Printf("failed to apply plan: %+v", err)
				goto failed
			}
		}

		log.Println("reconciliation successful!")

		a.lastAchievedMutex.Lock()
		a.lastAchievedDesiredState = time.Now()
		a.lastAchievedMutex.Unlock()

		goto end
	failed:
		select {
		case a.taintEvents <- struct{}{}:
		default:
		}

	end:
		a.inWarningState.Store(a.hasWarnings)
		time.Sleep(5 * time.Second)
	}
}

func NewAqueduct(ctx context.Context, clientset *kubernetes.Clientset,
	providers map[string]DNSProvider, ownerName string) *Aqueduct {
	informerFactory := informers.NewSharedInformerFactory(clientset, 0)

	aq := &Aqueduct{
		ctx: ctx,

		taintEvents:     make(chan struct{}, 1),
		informerFactory: informerFactory,
		nodesLister:     informerFactory.Core().V1().Nodes().Lister(),
		nodesInformer:   informerFactory.Core().V1().Nodes().Informer(),
		podsLister:      informerFactory.Core().V1().Pods().Lister(),
		podsInformer:    informerFactory.Core().V1().Pods().Informer(),
		serviceLister:   informerFactory.Core().V1().Services().Lister(),
		serviceInformer: informerFactory.Core().V1().Services().Informer(),
		providers:       providers,
		clientset:       clientset,
		ownerName:       ownerName,

		ipsToNodes: make(map[string]string),
	}

	return aq
}

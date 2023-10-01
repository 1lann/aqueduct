package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/labels"
)

type AqueductState struct {
	Sources map[string]*Source
}

func (a *AqueductState) String() string {
	data, err := json.Marshal(a)
	if err != nil {
		return fmt.Sprintf("failed to marshal state: %v", err)
	}

	return string(data)
}

type Node struct {
	Name      string
	IP        string
	dnsRecord DNSRecord
}

type Source struct {
	Domain      string
	Nodes       []Node
	Annotations map[string]string
	aqDNSRecord DNSRecord
}

func (a *Aqueduct) GetDesiredState() (*AqueductState, error) {
	desiredState := new(AqueductState)
	desiredState.Sources = make(map[string]*Source)

	services, err := a.serviceLister.List(labels.Everything())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list services")
	}

	for _, svc := range services {
		domain, found := svc.Annotations[aqueductAnnotation+"/domain"]
		if !found {
			continue
		}

		var nodes []Node

		targetPodNodes, found := svc.Annotations[aqueductAnnotation+"/target-pod-nodes"]
		if !found || targetPodNodes == "" || targetPodNodes == "false" || targetPodNodes == "0" {
			rawSelector, found := svc.Annotations[aqueductAnnotation+"/target-selector"]
			if !found {
				log.Printf("service %q has a domain annotation but is missing a target annotation, skipping", svc.Name)
				a.hasWarnings = true
				continue
			}

			selector, err := labels.Parse(rawSelector)
			if err != nil {
				log.Printf("service %q has a domain annotation but has an invalid target selector %q, skipping: %+v",
					svc.Name, rawSelector, err)
				a.hasWarnings = true
				continue
			}

			apiNodes, err := a.nodesLister.List(selector)
			if err != nil {
				log.Printf("service %q failed to list nodes with selector %q, skipping: %+v",
					svc.Name, rawSelector, err)
				a.hasWarnings = true
				continue
			}

			for _, node := range apiNodes {
				resolvedIP, err := a.ResolveHost(node.Name)
				if err != nil {
					log.Printf("service %q failed to resolve node %q, something seems wrong!", svc.Name, node.Name)
					panic("failed to resolve existing node")
				}

				nodes = append(nodes, Node{
					Name: node.Name,
					IP:   resolvedIP,
				})
			}
		} else {
			pods, err := a.podsLister.List(labels.SelectorFromSet(svc.Spec.Selector))
			if err != nil {
				log.Printf("service %q failed to list pods with selector %q, skipping: %+v",
					svc.Name, svc.Spec.Selector, err)
				a.hasWarnings = true
				continue
			}

			for _, pod := range pods {
				if pod.Spec.NodeName != "" {
					resolvedIP, err := a.ResolveHost(pod.Spec.NodeName)
					if err != nil {
						log.Printf("service %q failed to resolve node %q, something seems wrong!", svc.Name, pod.Spec.NodeName)
						panic("failed to resolve existing node")
					}

					nodes = append(nodes, Node{
						Name: pod.Spec.NodeName,
						IP:   resolvedIP,
					})
				}
			}
		}

		if _, found := desiredState.Sources[domain]; found {
			log.Printf("warning: duplicate domain %q", domain)
			desiredState.Sources[domain].Nodes = append(desiredState.Sources[domain].Nodes, nodes...)
			desiredState.Sources[domain].Annotations = mergeMap(desiredState.Sources[domain].Annotations, svc.Annotations)
		} else {
			log.Printf("discovered source %q with nodes %v", domain, svc.Annotations)
			desiredState.Sources[domain] = &Source{
				Domain:      domain,
				Nodes:       nodes,
				Annotations: svc.Annotations,
			}
		}
	}

	return desiredState, nil
}

func mergeMap(base, overlay map[string]string) map[string]string {
	baseCopy := make(map[string]string)
	for k, v := range base {
		baseCopy[k] = v
	}
	for k, v := range overlay {
		baseCopy[k] = v
	}

	return baseCopy
}

// DiscoverCurrentState discovers the current configured ingress state and updates currentState.
func (a *Aqueduct) DiscoverCurrentState() error {
	a.currentState = &AqueductState{
		Sources: make(map[string]*Source),
	}

	nodes, err := a.nodesLister.List(labels.Everything())
	if err != nil {
		return errors.Wrap(err, "failed to list nodes")
	}

	eg := new(errgroup.Group)
	// resolve all nodes to populate mappings
	for _, n := range nodes {
		name := n.Name
		eg.Go(func() error {
			hostIP, err := a.ResolveHost(name)
			if err == nil {
				log.Printf("resolved node %q -> %q", name, hostIP)
			} else {
				log.Printf("failed to resolve node %q: %v", name, err)
			}
			return errors.Wrapf(err, "failed to resolve node %q", name)
		})
	}
	if err := eg.Wait(); err != nil {
		log.Printf("one or more nodes failed to resolve: %v", err)
	}

	for domain, provider := range a.providers {
		log.Printf("checking records in provider %q for domain %q", provider.ProviderName(), domain)
		allRecords, err := provider.GetRecords(domain)
		if err != nil {
			log.Printf("failed to get AQ records: %+v", err)
			return errors.Wrapf(err, "get AQ records from provider %q", provider.ProviderName())
		}

		records := make(map[string][]DNSRecord)

		var aqRecords []DNSRecord
		for _, record := range allRecords {
			if record.Type() == "TXT" && strings.HasPrefix(record.Name(), "_aq.") {
				aqRecords = append(aqRecords, record)
			}

			if record.Type() != "A" {
				continue
			}

			records[record.Name()] = append(records[record.Name()], record)
		}

		for _, record := range aqRecords {
			log.Printf("discovered AQ record %s=%s", record.Name(), record.Value())

			if record.Value() != a.ownerName {
				continue
			}

			recordName := strings.TrimPrefix(record.Name(), "_aq.")
			currentDsts := records[recordName]

			currentDstNodes := make(map[string]DNSRecord)

			for _, dst := range currentDsts {
				currentDstNodes[a.LookupIP(dst.Value())] = dst
			}

			currentDstNodesSlice := make([]Node, 0, len(currentDstNodes))
			for node, dnsRecord := range currentDstNodes {
				currentDstNodesSlice = append(currentDstNodesSlice, Node{
					Name:      node,
					IP:        dnsRecord.Value(),
					dnsRecord: dnsRecord,
				})
			}

			a.currentState.Sources[recordName] = &Source{
				Domain:      recordName,
				Nodes:       currentDstNodesSlice,
				aqDNSRecord: record,
			}

			log.Printf("discovered existing source %q with nodes %v", recordName, currentDstNodesSlice)
		}
	}

	return nil
}

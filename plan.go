package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
)

type PlannedSource struct {
	Domain        string
	NodesToRemove []Node
	NodesToAdd    []Node
}

func (s *Source) Diff(b *Source) *PlannedSource {
	if s.Domain != b.Domain {
		panic("diffing sources with different domains")
	}

	plan := &PlannedSource{
		Domain: s.Domain,
	}

	for _, aNode := range s.Nodes {
		found := false
		for _, bNode := range b.Nodes {
			if aNode.IP == bNode.IP {
				found = true
				break
			}
		}

		if !found {
			plan.NodesToRemove = append(plan.NodesToRemove, aNode)
		}
	}

	for _, bNode := range b.Nodes {
		found := false
		for _, aNode := range s.Nodes {
			if aNode.IP == bNode.IP {
				found = true
				break
			}
		}

		if !found {
			plan.NodesToAdd = append(plan.NodesToAdd, bNode)
		}
	}

	if len(plan.NodesToAdd) == 0 && len(plan.NodesToRemove) == 0 {
		return nil
	}

	return plan
}

type Plan struct {
	SourcesToDelete []Source
	SourcesToAdd    []Source

	SourcesToModify map[string]*PlannedSource
}

func (p *Plan) String() string {
	data, err := json.Marshal(p)
	if err != nil {
		return fmt.Sprintf("failed to marshal plan: %v", err)
	}
	return string(data)
}

func (a *Aqueduct) GeneratePlan(desiredState *AqueductState) *Plan {
	plan := &Plan{
		SourcesToModify: make(map[string]*PlannedSource),
	}

	for domain, desiredSource := range desiredState.Sources {
		currentSource, ok := a.currentState.Sources[domain]
		if !ok {
			plan.SourcesToAdd = append(plan.SourcesToAdd, *desiredSource)
			plan.SourcesToModify[domain] = &PlannedSource{
				Domain:     domain,
				NodesToAdd: desiredSource.Nodes,
			}
			continue
		}

		diff := currentSource.Diff(desiredSource)
		if diff != nil {
			plan.SourcesToModify[domain] = diff
		}
	}

	for domain, currentSource := range a.currentState.Sources {
		_, ok := desiredState.Sources[domain]
		if !ok {
			plan.SourcesToDelete = append(plan.SourcesToDelete, *currentSource)
			plan.SourcesToModify[domain] = &PlannedSource{
				Domain:        domain,
				NodesToRemove: currentSource.Nodes,
			}
		}
	}

	if len(plan.SourcesToModify) == 0 && len(plan.SourcesToAdd) == 0 && len(plan.SourcesToDelete) == 0 {
		return nil
	}

	return plan
}

func splitDomain(domain string) (string, string) {
	parts := strings.Split(domain, ".")
	if len(parts) <= 2 {
		return "", domain
	}

	return strings.Join(parts[:len(parts)-2], "."), strings.Join(parts[len(parts)-2:], ".")
}

func (a *Aqueduct) GetProvider(domain string) DNSProvider {
	_, root := splitDomain(domain)
	provider, ok := a.providers[root]
	if !ok {
		return nil
	}
	return provider
}

func (a *Aqueduct) ExecutePlan(plan *Plan) error {
	for _, source := range plan.SourcesToDelete {
		provider := a.GetProvider(source.Domain)
		if provider == nil {
			log.Printf("no provider for %q, skipping", source)
			a.hasWarnings = true
			continue
		}

		for _, node := range plan.SourcesToModify[source.Domain].NodesToRemove {
			err := provider.DeleteRecord(node.dnsRecord)
			if err != nil {
				return errors.Wrapf(err, "delete A record %q from provider %q",
					source.Domain, provider.ProviderName())
			}
		}

		delete(plan.SourcesToModify, source.Domain)

		err := provider.DeleteRecord(source.aqDNSRecord)
		if err != nil {
			return errors.Wrapf(err, "delete AQ (TXT) record %q from provider %q",
				source.Domain, provider.ProviderName())
		}
	}

	for _, source := range plan.SourcesToAdd {
		provider := a.GetProvider(source.Domain)
		if provider == nil {
			log.Printf("no provider for %q, skipping", source)
			a.hasWarnings = true
			continue
		}

		err := provider.CreateRecord("_aq."+source.Domain, "TXT", a.ownerName)
		if err != nil {
			return errors.Wrapf(err, "create AQ (TXT) record %q from provider %q",
				source.Domain, provider.ProviderName())
		}
	}

	for domain, plannedSource := range plan.SourcesToModify {
		provider := a.GetProvider(domain)
		if provider == nil {
			log.Printf("no provider for %q, skipping", domain)
			a.hasWarnings = true
			continue
		}

		for len(plannedSource.NodesToAdd) > 0 && len(plannedSource.NodesToRemove) > 0 {
			err := provider.ReplaceRecord(plannedSource.NodesToRemove[0].dnsRecord,
				plannedSource.NodesToAdd[0].IP)
			if err != nil {
				return errors.Wrapf(err, "replace A record %q from provider %q",
					domain, provider.ProviderName())
			}
			plannedSource.NodesToRemove = plannedSource.NodesToRemove[1:]
			plannedSource.NodesToAdd = plannedSource.NodesToAdd[1:]
		}

		for _, node := range plannedSource.NodesToAdd {
			err := provider.CreateRecord(plannedSource.Domain, "A", node.IP)
			if err != nil {
				return errors.Wrapf(err, "create A record %q from provider %q",
					domain, provider.ProviderName())
			}
		}

		for _, node := range plannedSource.NodesToRemove {
			err := provider.DeleteRecord(node.dnsRecord)
			if err != nil {
				return errors.Wrapf(err, "delete A record %q from provider %q",
					domain, provider.ProviderName())
			}
		}
	}

	return nil
}

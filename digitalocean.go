package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/digitalocean/godo"
)

type DigitalOcean struct {
	client *godo.Client
}

func NewDigitalOcean(apiToken string) (*DigitalOcean, error) {
	client := godo.NewFromToken(apiToken)

	return &DigitalOcean{
		client: client,
	}, nil
}

func (c *DigitalOcean) ProviderName() string {
	return "digitalocean"
}

type DigitalOceanRecord struct {
	RootDomain string
	godo.DomainRecord
}

func (r *DigitalOceanRecord) Type() string {
	return r.DomainRecord.Type
}

func (r *DigitalOceanRecord) Name() string {
	return r.DomainRecord.Name + "." + r.RootDomain
}

func (r *DigitalOceanRecord) Value() string {
	return r.DomainRecord.Data
}

func (c *DigitalOcean) GetRecords(rootDomain string) ([]DNSRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	records, _, err := c.client.Domains.Records(ctx, rootDomain, &godo.ListOptions{
		PerPage: 200,
	})
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get records")
	}

	var returnRecords []DNSRecord
	for _, record := range records {
		returnRecords = append(returnRecords, &DigitalOceanRecord{
			RootDomain:   rootDomain,
			DomainRecord: record,
		})
	}

	return returnRecords, nil
}

func (c *DigitalOcean) CreateRecord(name, typ, value string) error {
	subDomain, root := splitDomain(name)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _, err := c.client.Domains.CreateRecord(ctx, root, &godo.DomainRecordEditRequest{
		Name: subDomain,
		Type: typ,
		Data: value,
		TTL:  30,
	})
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to create record")
	}

	return nil
}

func (c *DigitalOcean) DeleteRecord(record DNSRecord) error {
	doRecord, ok := record.(*DigitalOceanRecord)
	if !ok {
		return fmt.Errorf("record must be a *DigitalOceanRecord")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := c.client.Domains.DeleteRecord(ctx, doRecord.RootDomain, doRecord.DomainRecord.ID)
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to delete record")
	}

	return nil
}

func (c *DigitalOcean) ReplaceRecord(original DNSRecord, newValue string) error {
	doRecord, ok := original.(*DigitalOceanRecord)
	if !ok {
		return fmt.Errorf("record must be a *DigitalOceanRecord")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _, err := c.client.Domains.EditRecord(ctx, doRecord.RootDomain, doRecord.DomainRecord.ID,
		&godo.DomainRecordEditRequest{
			Name: doRecord.DomainRecord.Name,
			Type: doRecord.DomainRecord.Type,
			Data: newValue,
			TTL:  30,
		})
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to replace record")
	}

	return nil
}

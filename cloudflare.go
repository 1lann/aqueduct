package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cloudflare/cloudflare-go"
	"github.com/cockroachdb/errors"
)

type zoneIDCacheEntry struct {
	once sync.Once
	id   string
	err  error
}

type Cloudflare struct {
	client *cloudflare.API

	zoneIDCache      map[string]*zoneIDCacheEntry
	zoneIDCacheMutex sync.Mutex
}

func NewCloudflare(apiToken string) (*Cloudflare, error) {
	client, err := cloudflare.NewWithAPIToken(apiToken)
	if err != nil {
		return nil, err
	}

	return &Cloudflare{
		client:      client,
		zoneIDCache: make(map[string]*zoneIDCacheEntry),
	}, nil
}

func (c *Cloudflare) ProviderName() string {
	return "cloudflare"
}

func (c *Cloudflare) ZoneIDByName(name string) (string, error) {
	c.zoneIDCacheMutex.Lock()
	entry, ok := c.zoneIDCache[name]
	if !ok {
		entry = new(zoneIDCacheEntry)
		c.zoneIDCache[name] = entry
	}
	c.zoneIDCacheMutex.Unlock()

	entry.once.Do(func() {
		entry.id, entry.err = c.client.ZoneIDByName(name)

		if entry.err != nil {
			delete(c.zoneIDCache, name)
		}
	})

	return entry.id, entry.err
}

type CloudflareRecord struct {
	cloudflare.DNSRecord
}

func (r *CloudflareRecord) Type() string {
	return r.DNSRecord.Type
}

func (r *CloudflareRecord) Name() string {
	return r.DNSRecord.Name
}

func (r *CloudflareRecord) Value() string {
	return r.DNSRecord.Content
}

func (c *Cloudflare) GetRecords(rootDomain string) ([]DNSRecord, error) {
	zoneID, err := c.ZoneIDByName(rootDomain)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get zone ID")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	records, err := c.client.DNSRecords(ctx, zoneID, cloudflare.DNSRecord{})
	cancel()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get records")
	}

	var returnRecords []DNSRecord
	for _, record := range records {
		returnRecords = append(returnRecords, &CloudflareRecord{record})
	}

	return returnRecords, nil
}

func (c *Cloudflare) CreateRecord(name, typ, value string, annotations map[string]string) error {
	_, root := splitDomain(name)

	zoneID, err := c.ZoneIDByName(root)
	if err != nil {
		return errors.Wrap(err, "failed to get zone ID")
	}

	var useProxy bool
	if annotations[aqueductAnnotation+"/cloudflare-proxy"] == "true" {
		useProxy = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = c.client.CreateDNSRecord(ctx, zoneID, cloudflare.DNSRecord{
		Type:      typ,
		Name:      name,
		Content:   value,
		TTL:       60,
		Proxiable: useProxy,
	})
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to create record")
	}

	return nil
}

func (c *Cloudflare) DeleteRecord(record DNSRecord) error {
	cloudflareRecord, ok := record.(*CloudflareRecord)
	if !ok {
		return fmt.Errorf("record must be a *CloudflareRecord")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := c.client.DeleteDNSRecord(ctx, cloudflareRecord.ZoneID, cloudflareRecord.ID)
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to delete record")
	}

	return nil
}

func (c *Cloudflare) ReplaceRecord(original DNSRecord, newValue string, annotations map[string]string) error {
	cloudflareRecord, ok := original.(*CloudflareRecord)
	if !ok {
		return fmt.Errorf("record must be a *CloudflareRecord")
	}

	var useProxy bool
	if annotations[aqueductAnnotation+"/cloudflare-proxy"] == "true" {
		useProxy = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := c.client.UpdateDNSRecord(ctx, cloudflareRecord.ZoneID, cloudflareRecord.ID, cloudflare.DNSRecord{
		Content:   newValue,
		Proxiable: useProxy,
	})
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to replace record")
	}

	return nil
}

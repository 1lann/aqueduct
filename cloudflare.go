package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cloudflare/cloudflare-go/v4"
	"github.com/cloudflare/cloudflare-go/v4/dns"
	"github.com/cloudflare/cloudflare-go/v4/option"
	"github.com/cloudflare/cloudflare-go/v4/zones"
	"github.com/cockroachdb/errors"
)

type zoneIDCacheEntry struct {
	once sync.Once
	id   string
	err  error
}

type Cloudflare struct {
	client *cloudflare.Client

	zoneIDCache      map[string]*zoneIDCacheEntry
	zoneIDCacheMutex sync.Mutex
}

func NewCloudflare(apiToken string) (*Cloudflare, error) {
	client := cloudflare.NewClient(option.WithAPIToken(apiToken))

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
		resp, err := c.client.Zones.List(context.Background(), zones.ZoneListParams{
			Name: cloudflare.F(name),
		})
		entry.err = err

		if entry.err == nil && len(resp.Result) == 0 {
			entry.err = fmt.Errorf("zone %s not found", name)
		}

		if entry.err != nil {
			delete(c.zoneIDCache, name)
			return
		}

		entry.id = resp.Result[0].ID
	})

	return entry.id, entry.err
}

type CloudflareRecord struct {
	dns.RecordResponse
	ZoneID string
}

func (r *CloudflareRecord) Type() string {
	return string(r.RecordResponse.Type)
}

func (r *CloudflareRecord) Name() string {
	return r.RecordResponse.Name
}

func (r *CloudflareRecord) Value() string {
	switch v := r.RecordResponse.Data.(type) {
	case dns.ARecord:
		return v.Content
	case dns.TXTRecord:
		return v.Content
	default:
		panic(fmt.Sprintf("unknown record type: %T", v))
	}
}

func (c *Cloudflare) GetRecords(rootDomain string) ([]DNSRecord, error) {
	zoneID, err := c.ZoneIDByName(rootDomain)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get zone ID")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pages := c.client.DNS.Records.ListAutoPaging(ctx, dns.RecordListParams{
		ZoneID: cloudflare.F(zoneID),
	})

	var returnRecords []DNSRecord
	for pages.Next() {
		returnRecords = append(returnRecords, &CloudflareRecord{
			RecordResponse: pages.Current(),
			ZoneID:         zoneID,
		})
	}

	if err := pages.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to get records")
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
	if annotations[aqueductAnnotation+"/cloudflare-proxy"] == "true" && typ != "TXT" {
		log.Println("creating record with proxiable set to true for", name)
		useProxy = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	switch typ {
	case "A":
		_, err = c.client.DNS.Records.New(ctx, dns.RecordNewParams{
			ZoneID: cloudflare.F(zoneID),
			Record: dns.ARecordParam{
				Name:    cloudflare.F(name),
				Content: cloudflare.F(value),
				Proxied: cloudflare.F(useProxy),
				TTL:     cloudflare.F(dns.TTL(60)),
			},
		})
	case "TXT":
		_, err = c.client.DNS.Records.New(ctx, dns.RecordNewParams{
			ZoneID: cloudflare.F(zoneID),
			Record: dns.TXTRecordParam{
				Name:    cloudflare.F(name),
				Content: cloudflare.F(value),
			},
		})
	}
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
	_, err := c.client.DNS.Records.Delete(ctx, cloudflareRecord.ID, dns.RecordDeleteParams{
		ZoneID: cloudflare.F(cloudflareRecord.ZoneID),
	})
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

	var newRecord dns.RecordUnionParam
	switch cloudflareRecord.RecordResponse.Type {
	case dns.RecordResponseTypeA:
		newRecord = dns.ARecordParam{
			Content: cloudflare.F(newValue),
			Proxied: cloudflare.F(useProxy),
			TTL:     cloudflare.F(dns.TTL(60)),
		}
	case dns.RecordResponseTypeTXT:
		newRecord = dns.TXTRecordParam{
			Content: cloudflare.F(newValue),
		}
	default:
		return errors.Errorf("unknown record type: %s", cloudflareRecord.RecordResponse.Type)

	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := c.client.DNS.Records.Update(ctx, cloudflareRecord.ID, dns.RecordUpdateParams{
		ZoneID: cloudflare.F(cloudflareRecord.ZoneID),
		Record: newRecord,
	})
	cancel()
	if err != nil {
		return errors.Wrap(err, "failed to replace record")
	}

	return nil
}

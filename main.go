package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type CloudflareConfig struct {
	APITokenEnvVar string `json:"api_token_env_var"`
}

type ProviderConfig struct {
	Provider string          `json:"provider"`
	Options  json.RawMessage `json:"options"`
}

type Config struct {
	Providers map[string]ProviderConfig `json:"providers"`
	OwnerName string                    `json:"owner_name"`
}

func main() {
	if len(os.Args) < 2 {
		log.Println("usage: aqueduct <config file>")
		os.Exit(1)
		return
	}

	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	var cfg Config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		panic(err)
	}
	f.Close()

	if cfg.OwnerName == "" {
		cfg.OwnerName = "default"
	}

	kubeCfg, err := rest.InClusterConfig()
	if err != nil {
		log.Printf("failed to fetch implicit config, trying KUBECONFIG: %+v", err)
		kubeCfg, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
		if err != nil {
			panic(err)
		}
	}

	clientset, err := kubernetes.NewForConfig(kubeCfg)
	if err != nil {
		panic(err)
	}

	providers := make(map[string]DNSProvider)
	for name, providerCfg := range cfg.Providers {
		switch providerCfg.Provider {
		case "cloudflare":
			var cloudflareCfg CloudflareConfig
			if err := json.Unmarshal(providerCfg.Options, &cloudflareCfg); err != nil {
				panic(errors.Wrap(err, "parsing cloudflare config options"))
			}

			token := os.Getenv(cloudflareCfg.APITokenEnvVar)
			if token == "" {
				panic(errors.Errorf("missing cloudflare API token in %q", cloudflareCfg.APITokenEnvVar))
			}

			provider, err := NewCloudflare(token)
			if err != nil {
				panic(errors.Wrap(err, "creating cloudflare provider"))
			}

			providers[name] = provider
		}
	}

	ctx := context.Background()
	aq := NewAqueduct(ctx, clientset, providers, cfg.OwnerName)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if time.Since(aq.LastAchievedDesiredState()) > 10*time.Minute {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "unable to achieve desired state, last achieved: %s\n", aq.LastAchievedDesiredState().Format(time.RFC3339))
		} else if aq.IsInWarningState() {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("in warning state\n"))
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("ok\n"))
		}
	})

	http.HandleFunc("/livenessz", func(w http.ResponseWriter, r *http.Request) {
		if time.Since(aq.LastAchievedDesiredState()) > 10*time.Minute {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "unable to achieve desired state, last achieved: %s\n", aq.LastAchievedDesiredState().Format(time.RFC3339))
		} else {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "ok, last achieved desired staste: %s", aq.LastAchievedDesiredState().Format(time.RFC3339))
		}
	})

	go func() {
		port := os.Getenv("PORT")
		if port == "" {
			port = "4600"
		}
		log.Println("starting http server on :" + port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			panic(err)
		}
	}()

	aq.Reconciler()
}

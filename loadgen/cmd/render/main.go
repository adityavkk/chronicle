// Command render turns a declarative experiment spec into the Kubernetes
// manifests and Terraform vars for one load-test run.
//
//	render -spec loadtest/spec/sweep-10k.yaml -out loadtest/out
//
// With no -out it prints all three artifacts to stdout (handy for review/diff).
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"gecgithub01.walmart.com/auk000v/chronicle/loadgen/experiment"
)

func main() {
	specPath := flag.String("spec", "", "path to the experiment spec YAML (required)")
	outDir := flag.String("out", "", "output directory; empty prints to stdout")
	// Overrides for values that come from provisioning, not the spec author, so
	// the orchestrator (ltctl) can inject them without hand-editing the spec.
	redisURL := flag.String("redis-url", "", "override sut.redis_url (e.g. the provisioned Memorystore URL)")
	image := flag.String("image", "", "override sut.image")
	loadgenImage := flag.String("loadgen-image", "", "override loadgen_image")
	// replicas comes from the gate-#1 ramp (1->4), not the spec author, so ltctl
	// can re-render the same spec at each N without hand-editing it. 0 = keep the
	// spec's value.
	replicas := flag.Int("replicas", 0, "override sut.replicas (e.g. the gate-#1 1->4 ramp)")
	flag.Parse()

	if *specPath == "" {
		die(fmt.Errorf("-spec is required"))
	}
	data, err := os.ReadFile(*specPath)
	if err != nil {
		die(err)
	}
	spec, err := experiment.Load(data)
	if err != nil {
		die(err)
	}
	if *redisURL != "" {
		spec.SUT.RedisURL = *redisURL
	}
	if *image != "" {
		spec.SUT.Image = *image
	}
	if *loadgenImage != "" {
		spec.LoadgenImage = *loadgenImage
	}
	if *replicas > 0 {
		spec.SUT.Replicas = *replicas
	}
	r, err := spec.Render()
	if err != nil {
		die(err)
	}

	if *outDir == "" {
		fmt.Println("# === sut.yaml ===")
		fmt.Println(r.SUTManifest)
		fmt.Println("# === job.yaml ===")
		fmt.Println(r.JobManifest)
		fmt.Println("# === terraform.auto.tfvars ===")
		fmt.Print(r.TFVars)
		return
	}
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		die(err)
	}
	for name, content := range map[string]string{
		"sut.yaml":              r.SUTManifest,
		"job.yaml":              r.JobManifest,
		"terraform.auto.tfvars": r.TFVars,
	} {
		p := filepath.Join(*outDir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			die(err)
		}
		fmt.Fprintln(os.Stderr, "wrote", p)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "render:", err)
	os.Exit(1)
}

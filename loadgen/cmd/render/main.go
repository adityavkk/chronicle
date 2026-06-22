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
	// Fanout (gate #2) mode: render a FanoutSpec instead of a sweep Spec.
	fanout := flag.Bool("fanout", false, "render a fanout spec (gate #2) instead of a sweep spec")
	subs := flag.Int("subs", 0, "override fanout.subs (S value: 2/4/8/256)")
	sloP99Ms := flag.Float64("slo-p99-ms", 0, "override slo.fanout_p99_ms")
	flag.Parse()

	if *specPath == "" {
		die(fmt.Errorf("-spec is required"))
	}
	data, err := os.ReadFile(*specPath)
	if err != nil {
		die(err)
	}

	if *fanout {
		renderFanout(data, *outDir, *redisURL, *image, *loadgenImage, *subs, *sloP99Ms)
		return
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

// renderFanout renders the gate #2 fanout spec: a SUT manifest (reusing the
// sweep SUT template) plus the fanoutscale Job manifest. The TFVars are rendered
// by the sweep path; the CLUSTER for gate #2 is provisioned out of band
// (loadtest/terraform/cluster.tf, -var provision_gate2_cluster=true).
func renderFanout(data []byte, outDir, redisURL, image, loadgenImage string, subs int, sloP99Ms float64) {
	fs, err := experiment.LoadFanout(data)
	if err != nil {
		die(err)
	}
	if redisURL != "" {
		fs.SUT.RedisURL = redisURL
	}
	if image != "" {
		fs.SUT.Image = image
	}
	if loadgenImage != "" {
		fs.LoadgenImage = loadgenImage
	}
	if subs > 0 {
		fs.Fanout.Subs = subs
	}
	if sloP99Ms > 0 {
		fs.SLO.FanoutP99Ms = sloP99Ms
	}
	r, err := fs.Render()
	if err != nil {
		die(err)
	}
	if outDir == "" {
		fmt.Println("# === sut.yaml ===")
		fmt.Println(r.SUTManifest)
		fmt.Println("# === job.yaml ===")
		fmt.Println(r.JobManifest)
		return
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		die(err)
	}
	for name, content := range map[string]string{
		"sut.yaml": r.SUTManifest,
		"job.yaml": r.JobManifest,
	} {
		p := filepath.Join(outDir, name)
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

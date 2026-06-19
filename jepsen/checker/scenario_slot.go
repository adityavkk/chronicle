package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"gecgithub01.walmart.com/auk000v/chronicle/webhook"
)

// scenario_slot.go is the LIVE driver for T5 — no cross-subscriber leakage under
// slot-homing (07 line 46), the acceptance gate for #15's S-slot {__ds:h} state
// shard. Like ownership-exclusivity / shard-linz it drives webhook.RedisStore
// directly against a local Redis — NO cluster: the differential checker (the S-slot
// scatter-gather subscriber set EQUALS the unsharded reference) is a CORRECTNESS
// property that loopback proves; the gate-#2 fan-out p99 (the max-node-RTT number) is
// the only T5-adjacent claim that needs a real multi-node cluster, and that is
// loadtest gate #2, run separately (PENDING-CLOUD).
//
// It builds many subscriptions spread across the S keyspace slots, each linked to one
// of a few streams, then — WHILE an ownership-slot churn nemesis runs (orthogonal to
// the keyspace fan-out, so it must not perturb the result) — asserts for every stream:
//   - the scatter-gather subscriber set (store.StreamSubscribers, bitmap-gated) EQUALS
//     the independent reference set the harness linked,
//   - and EQUALS a brute-force union over all S per-slot fan-out shards (the bitmap
//     missed no occupied slot),
//   - with NO foreign id (a subscriber of another stream is never returned).
// Plus the static T5 precondition: every sub is whole-homed in ONE cluster slot, and a
// deliberately mis-tagged sub lands in a DIFFERENT slot (CROSSSLOT detected, not
// silent).

const t5BeginOffset = "0000000000000000_0000000000000000"

func t5Cfg(path string) webhook.Config {
	return webhook.Config{
		Type:       webhook.DispatchWebhook,
		Streams:    []string{path},
		WebhookURL: "http://127.0.0.1:1/unused", // never delivered; T5 is a fan-out read property
		LeaseTTLMs: 1000,
	}
}

// runSlotIsolation drives the T5 differential checker against local Redis.
func runSlotIsolation(c config) error {
	store, client, err := contentionStore(c)
	if err != nil {
		return err
	}
	defer client.Close()
	ctx := context.Background()

	nStreams := c.streams
	if nStreams < 2 {
		nStreams = 8
	}
	subsPerStream := 40 // ~8*40 = 320 subs, spread by fnv32a across the 256 slots
	runTag := time.Now().UnixNano()

	paths := make([]string, nStreams)
	refs := make(map[string][]string, nStreams)
	var allSubs []string
	for sIdx := 0; sIdx < nStreams; sIdx++ {
		path := fmt.Sprintf("events/t5-%d-%d", runTag, sIdx)
		paths[sIdx] = path
		for i := 0; i < subsPerStream; i++ {
			id := fmt.Sprintf("t5-%d-%d-%d", runTag, sIdx, i)
			links := []webhook.StreamLink{{Path: path, LinkType: webhook.LinkGlob, AckedOffset: t5BeginOffset}}
			if _, err := store.CreateOrConfirm(id, t5Cfg(path), links, time.Now()); err != nil {
				return fmt.Errorf("create t5 sub %s: %w", id, err)
			}
			refs[path] = append(refs[path], id)
			allSubs = append(allSubs, id)
		}
	}
	defer t5Cleanup(client, store, allSubs, paths, runTag)

	// How many distinct keyspace slots did the subscribers actually span? (Guards
	// against a vacuous run where everything homed to slot 0.)
	spanned := map[int]struct{}{}
	for _, id := range allSubs {
		spanned[dsSlotOf(id)] = struct{}{}
	}
	fmt.Printf("== slot-isolation (T5): subs=%d streams=%d spanning %d/%d keyspace slots ==\n",
		len(allSubs), nStreams, len(spanned), dsSubSlots)

	// Ownership-slot churn nemesis: concurrent claim_shard takeovers on a set of
	// ds:{ownership} slots, orthogonal to the {__ds:h} keyspace, running for the whole
	// verification. If the fan-out result moves under it, the two axes are not isolated.
	stopChurn := make(chan struct{})
	var churnWG sync.WaitGroup
	churnWG.Add(1)
	go func() { defer churnWG.Done(); t5OwnershipChurn(store, runTag, stopChurn) }()

	// Verify the differential repeatedly while the churn runs.
	const rounds = 12
	maxProbed := 0
	for r := 0; r < rounds; r++ {
		for _, path := range paths {
			scatter, slotsProbed, serr := store.StreamSubscribers(path)
			if serr != nil {
				close(stopChurn)
				churnWG.Wait()
				return fmt.Errorf("StreamSubscribers(%s): %w", path, serr)
			}
			if slotsProbed > maxProbed {
				maxProbed = slotsProbed
			}
			brute := t5BruteForceFanout(ctx, client, path)
			v := computeSlotLeakage(refs[path], scatter, brute)
			if !v.clean() {
				close(stopChurn)
				churnWG.Wait()
				return fmt.Errorf("T5 VIOLATED on %s (round %d): foreign=%v missing=%v brute-differ=%v "+
					"(scatter-gather subscriber set != reference / brute-force union)",
					path, r, trunc(v.Foreign), trunc(v.Missing), trunc(v.BruteDiffer))
			}
		}
	}
	close(stopChurn)
	churnWG.Wait()

	// Static T5 precondition: every sub whole-homed in ONE cluster slot.
	for _, id := range allSubs {
		if _, ok := subKeysOneSlot(id); !ok {
			return fmt.Errorf("T5 VIOLATED: sub %q is NOT whole-homed in one cluster slot (its keys CROSSSLOT)", id)
		}
	}

	// A deliberately mis-tagged sub is DETECTED, not silent: a fan-out entry placed in
	// the WRONG slot tag routes to a DIFFERENT cluster slot than the sub's home, so a
	// real cluster CROSSSLOTs it rather than silently co-locating.
	sample := allSubs[0]
	home := dsSlotOf(sample)
	correct := clusterSlot(dsStreamSubsKey(home, paths[0]))
	misTagged := clusterSlot(dsStreamSubsKey((home+1)%dsSubSlots, paths[0]))
	if correct == misTagged {
		return fmt.Errorf("T5 VIOLATED: a mis-tagged sub did not change cluster slot — CROSSSLOT would be silent, not detected")
	}

	fmt.Println("---- result ----")
	fmt.Printf("scenario:          %s\n", c.scenario)
	fmt.Printf("subscribers:       %d across %d streams, spanning %d/%d keyspace slots\n",
		len(allSubs), nStreams, len(spanned), dsSubSlots)
	fmt.Printf("max slots probed:  %d (the bitmap-gated fan-out width for the busiest stream)\n", maxProbed)
	fmt.Println("PASS: T5 no cross-subscriber leakage — the S-slot scatter-gather subscriber set EQUALS")
	fmt.Println("      the independent reference AND the brute-force all-S union for every stream, with")
	fmt.Println("      zero foreign wakes, held under concurrent ownership-slot churn (the two axes are")
	fmt.Println("      isolated); every sub is whole-homed in one cluster slot; a mis-tag is DETECTED")
	fmt.Println("      (CROSSSLOT), not silent.")
	return nil
}

// t5BruteForceFanout is the unsharded reference: the UNION of every per-slot fan-out
// shard for a path, read directly across all S slots WITHOUT consulting the
// occupied-slots bitmap. The implementation's bitmap-gated scatter-gather must equal
// this (else the bitmap missed an occupied slot). Pipelined into one batch.
func t5BruteForceFanout(ctx context.Context, client redis.UniversalClient, path string) []string {
	pipe := client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, dsSubSlots)
	for h := 0; h < dsSubSlots; h++ {
		cmds[h] = pipe.SMembers(ctx, dsStreamSubsKey(h, path))
	}
	_, _ = pipe.Exec(ctx)
	var out []string
	for _, cmd := range cmds {
		out = append(out, cmd.Val()...)
	}
	return out
}

// t5OwnershipChurn races claim_shard takeovers on a small set of ds:{ownership} slots
// with a short lease and rotating replica ids, bumping owner_epoch — the "slot-owner
// churn during the S-parallel fan-out" T5 nemesis (07). It writes ONLY the
// {ownership} keyspace, never {__ds:h}, so a correct fan-out is unaffected.
func t5OwnershipChurn(store *webhook.RedisStore, runTag int64, stop <-chan struct{}) {
	slotKeys := make([]string, 8)
	for h := range slotKeys {
		slotKeys[h] = fmt.Sprintf("ds:{ownership}:slot:t5-%d-%d", runTag, h)
	}
	ttl := 60 * time.Millisecond
	i := 0
	for {
		select {
		case <-stop:
			return
		default:
		}
		k := slotKeys[i%len(slotKeys)]
		replica := fmt.Sprintf("t5-churn-%d", i%3) // 3 contenders rotate ownership
		_, _ = store.ClaimSlot(k, replica, time.Now(), ttl)
		i++
		sleep(5 * time.Millisecond)
	}
}

func t5Cleanup(client redis.UniversalClient, store *webhook.RedisStore, subs, paths []string, runTag int64) {
	ctx := context.Background()
	for _, id := range subs {
		_ = store.Delete(id)
	}
	for _, p := range paths {
		client.Del(ctx, dsStreamSlotsKey(p)) // the bitmap is never cleared on deindex
		for h := 0; h < dsSubSlots; h++ {
			client.Del(ctx, dsStreamSubsKey(h, p))
		}
	}
	for h := 0; h < 8; h++ {
		client.Del(ctx, fmt.Sprintf("ds:{ownership}:slot:t5-%d-%d", runTag, h))
	}
}

// trunc keeps a counterexample list readable.
func trunc(xs []string) []string {
	sort.Strings(xs)
	if len(xs) > 8 {
		return append(xs[:8:8], "…")
	}
	return xs
}

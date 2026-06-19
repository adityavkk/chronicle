package main

import (
	"hash/fnv"
	"strconv"
)

const checkerSubSlots = 256

func checkerFNV32a(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func checkerSubscriptionSlot(id string) int {
	return int(checkerFNV32a(id) % checkerSubSlots)
}

func checkerKeyPrefix(slot int) string {
	return "ds:{__ds:" + strconv.Itoa(slot) + "}"
}

func checkerSubKey(id string) string {
	return checkerKeyPrefix(checkerSubscriptionSlot(id)) + ":sub:" + id
}

func checkerLeaseZKeyForSub(id string) string {
	return checkerKeyPrefix(checkerSubscriptionSlot(id)) + ":sched:lease"
}

func checkerOwnershipSlotKey(slot int) string {
	return checkerKeyPrefix(slot) + ":owner:slot:" + strconv.Itoa(slot)
}

func checkerStreamSubsKey(slot int, path string) string {
	return checkerKeyPrefix(slot) + ":stream:" + path
}

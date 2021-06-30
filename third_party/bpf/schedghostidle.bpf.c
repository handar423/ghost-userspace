// Copyright 2021 Google LLC
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// version 2 as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// vmlinux.h must be included before bpf_helpers.h
// clang-format off
#include "bpf/bpf/vmlinux_ghost.h"
#include "linux_tools/bpf_headers/bpf_core_read.h"
#include "linux_tools/bpf_headers/bpf_helpers.h"
#include "linux_tools/bpf_headers/bpf_tracing.h"
// clang-format on

#include "bpf/iovisor_bcc/bits.bpf.h"

#define MAX_CPUS 256
/* Keep this in sync with schedghostidle.c. */
#define NR_SLOTS 25

/*
 * This array maps is racy, but it's fine.  Both the latcher and sched_switch
 * tracepoints hold the RQ lock.  We want to access a cpu's data from another
 * cpu, since the latcher may not be on a particular cpu.
 */
struct cpu_info {
	bool is_idle;
	u64 idle_start;
};

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, MAX_CPUS);
	__type(key, u32);
	__type(value, struct cpu_info);
} cpu_info SEC(".maps");

/* key: hist slot idx.  value: count */
struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, NR_SLOTS);
	__type(key, u32);
	__type(value, u64);
} hist SEC(".maps");

static bool task_is_idle(struct task_struct *p)
{
	#define PF_IDLE 0x2	/* linux/sched.h */
	u32 flags = BPF_CORE_READ(p, flags);

	return flags & PF_IDLE;
}

SEC("tp_btf/sched_switch")
int BPF_PROG(sched_switch, bool preempt, struct task_struct *prev,
	     struct task_struct *next)
{
	u32 cpu = bpf_get_smp_processor_id();
	struct cpu_info *ci = bpf_map_lookup_elem(&cpu_info, &cpu);

	if (!ci)
		return 0;

	if (task_is_idle(next)) {
		ci->is_idle = true;
		ci->idle_start = bpf_ktime_get_ns();
	} else {
		ci->is_idle = false;
	}

	return 0;
}

static int task_cpu(struct task_struct *p)
{
	return BPF_CORE_READ(p, cpu);
}

static void update_hist(u64 nsec)
{
	u64 slot, *count;

	slot = log2l(nsec / 1000);
	if (slot >= NR_SLOTS)
		slot = NR_SLOTS - 1;
	count = bpf_map_lookup_elem(&hist, &slot);
	if (!count)
		return;
	*count += 1;
}

SEC("tp_btf/sched_ghost_latched")
int BPF_PROG(sched_ghost_latched, struct task_struct *old,
	     struct task_struct *new)
{
	u32 cpu = task_cpu(new);
	struct cpu_info *ci = bpf_map_lookup_elem(&cpu_info, &cpu);

	if (!ci)
		return 0;

	if (!ci->is_idle)
		return 0;
	update_hist(bpf_ktime_get_ns() - ci->idle_start);
	/*
	 * Technically, the cpu is still idle, and our latch may get aborted or
	 * otherwise fail.  But the agent has noticed the previous idling (as
	 * shown by it trying to latch), so we do not want to count as idle for
	 * any other latchings that happen before the next sched_switch.
	 */
	ci->is_idle = false;

	return 0;
}

char LICENSE[] SEC("license") = "GPL";
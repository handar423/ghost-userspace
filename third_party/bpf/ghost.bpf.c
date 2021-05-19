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
#include "linux_tools/bpf_headers/vmlinux.h"
#include "linux_tools/bpf_headers/bpf_helpers.h"
#include "linux_tools/bpf_headers/bpf_tracing.h"
// clang-format on

#include "bpf/bpf/ghost_shared_bpf.h"

/* max_entries is patched at runtime to num_possible_cpus */
struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, 1024);
	__type(key, u32);
	__type(value, struct per_cpu_data);
	__uint(map_flags, BPF_F_MMAPABLE);
} cpu_data SEC(".maps");


SEC("scheduler/tick")
int sched_tick(struct bpf_scheduler *ctx)
{
	struct per_cpu_data *my_data;
	u32 cpu = bpf_get_smp_processor_id();

	my_data = bpf_map_lookup_elem(&cpu_data, &cpu);
	if (!my_data)
		return 0;
	if (!my_data->want_tick)
		return 0;
	my_data->want_tick = false;

	return 1;
}

char LICENSE[] SEC("license") = "GPL";

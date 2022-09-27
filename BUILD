# Note: If you modify this BUILD file, please contact jhumphri@ first to ensure
# that you are not breaking the Copybara script.

load("//:bpf/bpf.bzl", "bpf_skeleton")

package(default_visibility = ["//:__pkg__"])

# Each license covers the code below:
#
# BSD 2: Just covers the IOVisor BCC code in third_party/iovisor_bcc/. This
# code was not written by Google.
#
# GPLv2: Just covers the eBPF code in third_party/bpf/. This code was written
# by Google. We need to license it under GPLv2 though so that the eBPF code
# can use kernel functionality restricted to code licensed under GPLv2.
#
# Apache 2: All other code is covered by Apache 2. This includes the library
# code in lib/, the experiments, all code in bpf/user/, etc.
licenses(["notice"])

exports_files(["LICENSE"])

compiler_flags = [
    "-Wno-sign-compare",
    "-DGHOST_LOGGING",
]

bpf_linkopts = [
    "-lelf",
    "-lz",
]

cc_library(
    name = "agent",
    srcs = [
        "bpf/user/agent.c",
        "lib/agent.cc",
        "lib/channel.cc",
        "lib/enclave.cc",
        "lib/scheduler.cc",
        "lib/topology.cc",
    ],
    hdrs = [
        "bpf/user/agent.h",
        "bpf/user/ghost_bpf.skel.h",
        "lib/agent.h",
        "lib/channel.h",
        "lib/enclave.h",
        "lib/scheduler.h",
        "lib/topology.h",
        "//third_party:iovisor_bcc/trace_helpers.h",
        "//third_party/bpf:ghost.h",
    ],
    copts = compiler_flags,
    linkopts = bpf_linkopts + ["-lnuma"],
    deps = [
        ":base",
        ":ghost",
        ":shared",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@linux//:libbpf",
    ],
)

cc_binary(
    name = "agent_exp",
    srcs = [
        "schedulers/edf/agent_exp.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":edf_scheduler",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/flags:parse",
    ],
)

cc_library(
    name = "shinjuku_scheduler",
    srcs = [
        "schedulers/shinjuku/shinjuku_orchestrator.cc",
        "schedulers/shinjuku/shinjuku_scheduler.cc",
    ],
    hdrs = [
        "schedulers/shinjuku/shinjuku_orchestrator.h",
        "schedulers/shinjuku/shinjuku_scheduler.h",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":ghost",
        ":shared",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_binary(
    name = "agent_shinjuku",
    srcs = [
        "schedulers/shinjuku/agent_shinjuku.cc",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":agent",
        ":shinjuku_scheduler",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/flags:parse",
    ],
)

cc_library(
    name = "flex_scheduler",
    srcs = [
        "schedulers/flex/flex_orchestrator.cc",
        "schedulers/flex/flex_scheduler.cc",
    ],
    hdrs = [
        "schedulers/flex/flex_orchestrator.h",
        "schedulers/flex/flex_scheduler.h",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":ghost",
        ":shared",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_binary(
    name = "agent_flex",
    srcs = [
        "schedulers/flex/agent_flex.cc",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":agent",
        ":flex_scheduler",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/flags:parse",
    ],
)

cc_library(
    name = "sol_scheduler",
    srcs = [
        "schedulers/sol/sol_scheduler.cc",
    ],
    hdrs = [
        "schedulers/sol/sol_scheduler.h",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_binary(
    name = "agent_sol",
    srcs = [
        "schedulers/sol/agent_sol.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":sol_scheduler",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/flags:parse",
    ],
)

cc_binary(
    name = "sol_test",
    srcs = [
        "tests/sol_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":ghost",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "agent_test",
    size = "small",
    srcs = [
        "tests/agent_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/random",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "api_test",
    size = "small",
    srcs = [
        "tests/api_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":fifo_scheduler",
        "@com_google_absl//absl/random",
        "@com_google_googletest//:gtest_main",
    ],
)

# Makes vmlinux_ghost_*.h files visible to eBPF code.
exports_files(glob(["kernel/vmlinux_ghost_*.h"]))

cc_library(
    name = "base",
    srcs = [
        "lib/base.cc",
    ],
    hdrs = [
        "kernel/ghost_uapi.h",
        "lib/base.h",
        "lib/logging.h",
    ],
    copts = compiler_flags,
    deps = [
        "@com_google_absl//absl/base",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:node_hash_map",
        "@com_google_absl//absl/debugging:stacktrace",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "base_test",
    size = "small",
    srcs = [
        "tests/base_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "capabilities_test",
    size = "small",
    srcs = [
        "tests/capabilities_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":capabilities_test_lib",
        ":ghost",
    ],
)

cc_library(
    name = "capabilities_test_lib",
    testonly = 1,
    hdrs = [
        "tests/capabilities_test.h",
    ],
    copts = compiler_flags,
    linkopts = ["-lcap"],
    deps = ["@com_google_googletest//:gtest_main"],
)

cc_test(
    name = "channel_test",
    size = "small",
    srcs = [
        "tests/channel_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        "@com_google_googletest//:gtest_main",
    ],
)

bpf_skeleton(
    name = "edf_bpf_skel",
    bpf_object = "//third_party/bpf:edf_bpf",
    skel_hdr = "schedulers/edf/edf_bpf.skel.h",
)

cc_library(
    name = "edf_scheduler",
    srcs = [
        "schedulers/edf/edf_scheduler.cc",
        "schedulers/edf/orchestrator.cc",
    ],
    hdrs = [
        "schedulers/edf/edf_bpf.skel.h",
        "schedulers/edf/edf_scheduler.h",
        "schedulers/edf/orchestrator.h",
        "//third_party/bpf:edf.h",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":ghost",
        ":shared",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/strings:str_format",
        "@linux//:libbpf",
    ],
)

cc_test(
    name = "edf_test",
    size = "small",
    srcs = [
        "tests/edf_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":edf_scheduler",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "enclave_test",
    size = "small",
    srcs = [
        "tests/enclave_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "fifo_agent",
    srcs = [
        "schedulers/fifo/fifo_agent.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":fifo_scheduler",
        "@com_google_absl//absl/debugging:symbolize",
        "@com_google_absl//absl/flags:parse",
    ],
)

cc_library(
    name = "fifo_scheduler",
    srcs = [
        "schedulers/fifo/fifo_scheduler.cc",
        "schedulers/fifo/fifo_scheduler.h",
    ],
    hdrs = [
        "schedulers/fifo/fifo_scheduler.h",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
    ],
)

cc_library(
    name = "ghost",
    srcs = [
        "lib/ghost.cc",
        "lib/topology.cc",
    ],
    hdrs = [
        "kernel/ghost_uapi.h",
        "lib/ghost.h",
        "lib/topology.h",
    ],
    copts = compiler_flags,
    linkopts = ["-lnuma"],
    deps = [
        ":base",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/strings:str_format",
    ],
)

cc_test(
    name = "prio_table_test",
    size = "small",
    srcs = [
        "tests/prio_table_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":shared",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "shared",
    srcs = [
        "shared/prio_table.cc",
        "shared/shmem.cc",
    ],
    hdrs = [
        "shared/prio_table.h",
        "shared/shmem.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/strings",
    ],
)

cc_test(
    name = "topology_test",
    size = "small",
    srcs = [
        "tests/topology_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":agent",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest",
    ],
)

bpf_skeleton(
    name = "ghost_bpf_skel",
    bpf_object = "//third_party/bpf:ghost_bpf",
    skel_hdr = "bpf/user/ghost_bpf.skel.h",
)

bpf_skeleton(
    name = "schedclasstop_bpf_skel",
    bpf_object = "//third_party/bpf:schedclasstop_bpf",
    skel_hdr = "bpf/user/schedclasstop_bpf.skel.h",
)

cc_binary(
    name = "schedclasstop",
    srcs = [
        "bpf/user/schedclasstop.c",
        "bpf/user/schedclasstop_bpf.skel.h",
        "//third_party:iovisor_bcc/trace_helpers.h",
    ],
    copts = compiler_flags,
    linkopts = bpf_linkopts,
    deps = [
        "@linux//:libbpf",
    ],
)

bpf_skeleton(
    name = "schedghostidle_bpf_skel",
    bpf_object = "//third_party/bpf:schedghostidle_bpf",
    skel_hdr = "bpf/user/schedghostidle_bpf.skel.h",
)

cc_binary(
    name = "schedghostidle",
    srcs = [
        "bpf/user/schedghostidle.c",
        "bpf/user/schedghostidle_bpf.skel.h",
        "//third_party:iovisor_bcc/trace_helpers.h",
    ],
    copts = compiler_flags,
    linkopts = bpf_linkopts,
    deps = [
        "@linux//:libbpf",
    ],
)

bpf_skeleton(
    name = "schedlat_bpf_skel",
    bpf_object = "//third_party/bpf:schedlat_bpf",
    skel_hdr = "bpf/user/schedlat_bpf.skel.h",
)

cc_binary(
    name = "schedlat",
    srcs = [
        "bpf/user/schedlat.c",
        "bpf/user/schedlat_bpf.skel.h",
        "//third_party:iovisor_bcc/trace_helpers.h",
        "//third_party/bpf:schedlat.h",
    ],
    copts = compiler_flags,
    linkopts = bpf_linkopts,
    deps = [
        "@linux//:libbpf",
    ],
)

bpf_skeleton(
    name = "schedrun_bpf_skel",
    bpf_object = "//third_party/bpf:schedrun_bpf",
    skel_hdr = "bpf/user/schedrun_bpf.skel.h",
)

cc_binary(
    name = "schedrun",
    srcs = [
        "bpf/user/schedrun.c",
        "bpf/user/schedrun_bpf.skel.h",
        "//third_party:iovisor_bcc/trace_helpers.h",
        "//third_party/bpf:schedrun.h",
    ],
    copts = compiler_flags,
    linkopts = bpf_linkopts,
    deps = [
        "@linux//:libbpf",
    ],
)

# Shared library for ghOSt tests.

cc_library(
    name = "experiments_shared",
    srcs = [
        "experiments/shared/cfs.cc",
        "experiments/shared/ghost.cc",
        "experiments/shared/thread_pool.cc",
    ],
    hdrs = [
        "experiments/shared/cfs.h",
        "experiments/shared/ghost.h",
        "experiments/shared/thread_pool.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":ghost",
        ":shared",
    ],
)

cc_test(
    name = "thread_pool_test",
    size = "small",
    srcs = [
        "experiments/shared/thread_pool.cc",
        "experiments/shared/thread_pool.h",
        "experiments/shared/thread_pool_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":ghost",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

# The RocksDB binary and tests.

cc_binary(
    name = "rocksdb",
    srcs = [
        "experiments/rocksdb/cfs_orchestrator.cc",
        "experiments/rocksdb/cfs_orchestrator.h",
        "experiments/rocksdb/clock.h",
        "experiments/rocksdb/database.cc",
        "experiments/rocksdb/database.h",
        "experiments/rocksdb/ghost_orchestrator.cc",
        "experiments/rocksdb/ghost_orchestrator.h",
        "experiments/rocksdb/ingress.cc",
        "experiments/rocksdb/ingress.h",
        "experiments/rocksdb/latency.cc",
        "experiments/rocksdb/latency.h",
        "experiments/rocksdb/main.cc",
        "experiments/rocksdb/orchestrator.cc",
        "experiments/rocksdb/orchestrator.h",
        "experiments/rocksdb/request.h",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@rocksdb",
    ],
)

cc_test(
    name = "latency_test",
    size = "small",
    srcs = [
        "experiments/rocksdb/latency.cc",
        "experiments/rocksdb/latency.h",
        "experiments/rocksdb/latency_test.cc",
        "experiments/rocksdb/request.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "rocksdb_options_test",
    size = "small",
    srcs = [
        "experiments/rocksdb/cfs_orchestrator.cc",
        "experiments/rocksdb/cfs_orchestrator.h",
        "experiments/rocksdb/clock.h",
        "experiments/rocksdb/database.cc",
        "experiments/rocksdb/database.h",
        "experiments/rocksdb/ghost_orchestrator.cc",
        "experiments/rocksdb/ghost_orchestrator.h",
        "experiments/rocksdb/ingress.cc",
        "experiments/rocksdb/ingress.h",
        "experiments/rocksdb/latency.cc",
        "experiments/rocksdb/latency.h",
        "experiments/rocksdb/options_test.cc",
        "experiments/rocksdb/orchestrator.cc",
        "experiments/rocksdb/orchestrator.h",
        "experiments/rocksdb/request.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@rocksdb",
    ],
)

cc_test(
    name = "rocksdb_orchestrator_test",
    size = "small",
    srcs = [
        "experiments/rocksdb/cfs_orchestrator.cc",
        "experiments/rocksdb/cfs_orchestrator.h",
        "experiments/rocksdb/clock.h",
        "experiments/rocksdb/database.cc",
        "experiments/rocksdb/database.h",
        "experiments/rocksdb/ghost_orchestrator.cc",
        "experiments/rocksdb/ghost_orchestrator.h",
        "experiments/rocksdb/ingress.cc",
        "experiments/rocksdb/ingress.h",
        "experiments/rocksdb/latency.cc",
        "experiments/rocksdb/latency.h",
        "experiments/rocksdb/orchestrator.cc",
        "experiments/rocksdb/orchestrator.h",
        "experiments/rocksdb/orchestrator_test.cc",
        "experiments/rocksdb/request.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@rocksdb",
    ],
)

cc_test(
    name = "database_test",
    size = "small",
    srcs = [
        "experiments/rocksdb/database.cc",
        "experiments/rocksdb/database.h",
        "experiments/rocksdb/database_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_googletest//:gtest",
        "@rocksdb",
    ],
)

cc_test(
    name = "synthetic_network_test",
    size = "medium",
    srcs = [
        "experiments/rocksdb/clock.h",
        "experiments/rocksdb/database.h",
        "experiments/rocksdb/ingress.cc",
        "experiments/rocksdb/ingress.h",
        "experiments/rocksdb/request.h",
        "experiments/rocksdb/synthetic_network_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@rocksdb",
    ],
)

# The Antagonist binary and tests.

cc_binary(
    name = "antagonist",
    srcs = [
        "experiments/antagonist/cfs_orchestrator.cc",
        "experiments/antagonist/cfs_orchestrator.h",
        "experiments/antagonist/ghost_orchestrator.cc",
        "experiments/antagonist/ghost_orchestrator.h",
        "experiments/antagonist/main.cc",
        "experiments/antagonist/orchestrator.cc",
        "experiments/antagonist/orchestrator.h",
        "experiments/antagonist/results.cc",
        "experiments/antagonist/results.h",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_test(
    name = "antagonist_options_test",
    size = "small",
    srcs = [
        "experiments/antagonist/options_test.cc",
        "experiments/antagonist/orchestrator.cc",
        "experiments/antagonist/orchestrator.h",
        "experiments/antagonist/results.cc",
        "experiments/antagonist/results.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "antagonist_orchestrator_test",
    size = "small",
    srcs = [
        "experiments/antagonist/orchestrator.cc",
        "experiments/antagonist/orchestrator.h",
        "experiments/antagonist/orchestrator_test.cc",
        "experiments/antagonist/results.cc",
        "experiments/antagonist/results.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "results_test",
    size = "small",
    srcs = [
        "experiments/antagonist/results.cc",
        "experiments/antagonist/results.h",
        "experiments/antagonist/results_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "global_scalability",
    srcs = [
        "experiments/microbenchmarks/global_scalability.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":edf_scheduler",
        ":shinjuku_scheduler",
        ":flex_scheduler",
        ":sol_scheduler",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/flags:usage",
    ],
)

cc_test(
    name = "ioctl_test",
    size = "small",
    srcs = ["experiments/microbenchmarks/ioctl_test.cc"],
    copts = compiler_flags,
    deps = [
        ":agent",
        ":ghost",
        "@com_google_benchmark//:benchmark",
        "@com_google_googletest//:gtest",
    ],
)



# rocksdb without ghost 
cc_binary(
    name = "rocksdb_origin",
    srcs = [
        "experiments/rocksdb_origin/clock.h",
        "experiments/rocksdb_origin/database.cc",
        "experiments/rocksdb_origin/database.h",
        "experiments/rocksdb_origin/ingress.cc",
        "experiments/rocksdb_origin/ingress.h",
        "experiments/rocksdb_origin/latency.cc",
        "experiments/rocksdb_origin/latency.h",
        "experiments/rocksdb_origin/main.cc",
        "experiments/rocksdb_origin/orchestrator.cc",
        "experiments/rocksdb_origin/orchestrator.h",
        "experiments/rocksdb_origin/request.h",
        "experiments/rocksdb_origin/thread_pool.cc",
        "experiments/rocksdb_origin/thread_pool.h",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":ghost",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@rocksdb",
    ],
)

cc_test(
    name = "database_origin_test",
    size = "small",
    srcs = [
        "experiments/rocksdb_origin/database.cc",
        "experiments/rocksdb_origin/database.h",
        "experiments/rocksdb_origin/database_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/flags:flag",
        "@com_google_absl//absl/flags:parse",
        "@com_google_googletest//:gtest",
        "@rocksdb",
    ],
)

cc_test(
    name = "latency_origin_test",
    size = "small",
    srcs = [
        "experiments/rocksdb_origin/latency.cc",
        "experiments/rocksdb_origin/latency.h",
        "experiments/rocksdb_origin/latency_test.cc",
        "experiments/rocksdb_origin/request.h",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "synthetic_network_origin_test",
    size = "large",
    srcs = [
        "experiments/rocksdb_origin/clock.h",
        "experiments/rocksdb_origin/database.h",
        "experiments/rocksdb_origin/ingress.cc",
        "experiments/rocksdb_origin/ingress.h",
        "experiments/rocksdb_origin/request.h",
        "experiments/rocksdb_origin/synthetic_network_test.cc",
    ],
    copts = compiler_flags,
    deps = [
        ":base",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@rocksdb",
    ],
)


cc_binary(
    name = "libhijack.so", #mylib是头文件的名字
    srcs = [
        "experiments/hijack/ghost_status.h",
        "experiments/hijack/ghost_status.cc",
        "experiments/hijack/sem_wait.cc",
        "experiments/hijack/sem_wait.h",
        "experiments/hijack/pthread_setscheduparam.cc",
        "experiments/hijack/pthread_setscheduparam.h",
        "experiments/hijack/pthread_setaffinity_np.cc",
        "experiments/hijack/pthread_setaffinity_np.h",
    ],
    deps = [
        ":base",
        ":experiments_shared",
        ":ghost",
        "@com_google_absl//absl/time",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    linkopts = [
        "-lstdc++",
        "-ldl",
        "-fPIC",
        "-shared", #链接时候的命令
    ],
    linkshared = True,
    linkstatic = True,
)

cc_binary(
    name = "libShinjukuHijack.so", #mylib是头文件的名字
    srcs = [
        "experiments/hijack/ghost_status.h",
        "experiments/hijack/ghost_status.cc",
        "experiments/hijack/pthread_setscheduparam.cc",
        "experiments/hijack/pthread_setscheduparam.h",
        "experiments/hijack/pthread_setaffinity_np.cc",
        "experiments/hijack/pthread_setaffinity_np.h",
    ],
    deps = [
        ":base",
        ":experiments_shared",
        ":ghost",
        "@com_google_absl//absl/time",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    linkopts = [
        "-lstdc++",
        "-ldl",
        "-fPIC",
        "-shared", #链接时候的命令
    ],
    linkshared = True,
    linkstatic = True,
)


cc_binary(
    name = "libNginxHijack.so", #mylib是头文件的名字
    srcs = [
        "experiments/hijack/ghost_status.h",
        "experiments/hijack/ghost_status.cc",
        "experiments/hijack/sched_setaffinity.cc",
        "experiments/hijack/sched_setaffinity.h",
    ],
    deps = [
        ":base",
        ":experiments_shared",
        ":ghost",
        "@com_google_absl//absl/time",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    linkopts = [
        "-lstdc++",
        "-ldl",
        "-fPIC",
        "-shared", #链接时候的命令
    ],
    linkshared = True,
    linkstatic = True,
)

cc_binary(
    name = "libLinuxHijack.so", #mylib是头文件的名字
    srcs = [
        "experiments/hijack/ghost_status.h",
        "experiments/hijack/ghost_status.cc",
        "experiments/hijack/pthread_setscheduparam.cc",
        "experiments/hijack/pthread_setscheduparam.h",
    ],
    deps = [
        ":base",
        ":experiments_shared",
        ":ghost",
        "@com_google_absl//absl/time",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    linkopts = [
        "-lstdc++",
        "-ldl",
        "-fPIC",
        "-shared", #链接时候的命令
    ],
    linkshared = True,
    linkstatic = True,
)

cc_binary(
    name = "rocksdb_single",
    srcs = [
        "experiments/rocksdb_single/cfs_orchestrator.cc",
        "experiments/rocksdb_single/cfs_orchestrator.h",
        "experiments/rocksdb_single/clock.h",
        "experiments/rocksdb_single/database.cc",
        "experiments/rocksdb_single/database.h",
        "experiments/rocksdb_single/ghost_orchestrator.cc",
        "experiments/rocksdb_single/ghost_orchestrator.h",
        "experiments/rocksdb_single/ingress.cc",
        "experiments/rocksdb_single/ingress.h",
        "experiments/rocksdb_single/latency.cc",
        "experiments/rocksdb_single/latency.h",
        "experiments/rocksdb_single/main.cc",
        "experiments/rocksdb_single/orchestrator.cc",
        "experiments/rocksdb_single/orchestrator.h",
        "experiments/rocksdb_single/request.h",
        "experiments/rocksdb_single/thread_pool.h",
    ],
    copts = compiler_flags,
    visibility = ["//experiments/scripts:__pkg__"],
    deps = [
        ":base",
        ":experiments_shared",
        "@com_google_absl//absl/flags:parse",
        "@com_google_absl//absl/functional:bind_front",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/random:bit_gen_ref",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@rocksdb",
    ],
)
/root/ghost-userspace/bazel-bin/rocksdb --print_format pretty --noprint_distribution  --noprint_ns  --print_get  --print_range  --rocksdb_db_path /dev/shm/orch_db --range_query_ratio 0.005 --load_generator_cpu 10 --cfs_dispatcher_cpu 11 --num_workers 5 --worker_cpus '' --cfs_wait_type futex --get_duration 10us --range_duration 5000us --get_exponential_mean 0us --batch 1 --experiment_duration 15s --discard_duration 2s --scheduler cfs --throughput 10000 --worker_cpus 2,3,4,5,6

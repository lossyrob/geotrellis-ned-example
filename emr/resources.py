import math

image_count = 1115
image_size = 10812
image_size_in_bytes = image_size * image_size * 8


chunk_size = 1024
chunk_size_in_bytes = chunk_size * chunk_size * 8

chunk_count = 1115 * (math.pow(math.ceil(image_size / chunk_size), 2))

print chunk_count
print chunk_size_in_bytes

target_partition_size = 268400000

chunks_per_partition = int(math.ceil(target_partition_size / chunk_size_in_bytes))
num_partition = chunk_count / chunks_per_partition

num_workers = 50
worker_cores = 8
worker_mem = 30000 - 5000
mem_per_worker = worker_mem / worker_cores
mem_without_overhead = mem_per_worker / 1.2

print "EXECUTORS:        %d" % (num_workers * worker_cores)
print "EXECUTOR_MEMORY:  %fm" % mem_without_overhead
print "YARN OVERHEAD:    %fm" % (mem_without_overhead * 0.20)

print "NUM PARTITION:    %d" % num_partition

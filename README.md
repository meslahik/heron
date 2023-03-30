# Heron
Heron is developed as part of a research project on partitioned state machine replication and RDMA.
Heron's paper is published at DSN 2023.

Heron: Scalable State Machine Replication on Shared Memory
The paper introduces Heron is a state machine replication system that delivers scalable throughput and microsecond latency. 
Heron achieves scalability through partitioning (sharding) and microsecond latency through a careful design that leverages one-sided RDMA primitives. 
Heron significantly improves the throughput and latency of applications when compared to message passing-based replicated systems. 
But it really shines when executing multi-partition requests, where objects in multiple partitions are accessed in a request, the Achilles heel of most partitioned systems. 
We implemented Heron and evaluated its performance extensively. 
Our experiments show that Heron reduces the latency of coordinating linearizable executions to the level of microseconds and improves the performance of executing complex workloads by one order of magnitude in comparison to state-of-the-art S-SMR systems.

This repositoy contains 2 projects:
- Heron: Scalable State Machine Replication on Shared Memory
- HeronTpcc: A key-value store version of TPCC workload running on Heron

# Delta CRDT Dissemination Testbed

A containerized Rust testbed evaluating dissemination strategies (proactive, reactive, hybrid) for Delta-State CRDTs across varied network topologies.

Tech-Stack: Rust (Core), Docker/ Docker Compose (Orchestration), TCP (Networking)


### Quick Start
```bash
cargo build --release
./experiments.sh
```

### Empirical Results
* **Proactive (Push):** Fastest convergence, but creates severe network/CPU bottlenecks in dense topologies
* **Reactive (Pull):** Highly network-efficient, but suffers from slow convergence in sparse/high-diameter networks
* **Hybrid (Push-Pull):** Balances speed and load, but introduces redundant messages

### Limitations
* **Network Realities:** Runs on a single host; does not simulate packet loss, latency, or partitions
* **Convergence Measurement:** Uses host system timestamps; a real distributed environment requires logical clocks to account for drift
* **Data Structure:** Uses a standard OR-Set; compute overhead will scale differently with more complex structures

This is a merely a controlled academic simulation.

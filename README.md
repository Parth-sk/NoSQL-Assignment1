# SimuFragDB: Distributed Database Simulation

## Overview
SimuFragDB is a simulated distributed relational database system. It employs **Horizontal Fragmentation** to partition student and grade data across multiple PostgreSQL instances (fragments). The system uses a deterministic routing function to manage data placement and retrieval.

## System Architecture
* **Driver**: Parses the `workload.txt` and dispatches commands to the client.
* **FragmentClient**: Acts as a middleware/proxy that handles the logic of routing and aggregation.
* **Router**: Uses a hash-based deterministic function (`|hash(key)| % N`) to map student IDs to specific fragments.
* **Storage**: Multiple independent PostgreSQL databases representing individual nodes.

## Implementation Details

### Data Routing
For all point-based operations (`INSERT`, `UPDATE`, `DELETE`, and `READ_PROFILE`), the system uses the `Router` to identify the specific fragment. This ensures that a student's data is always consistent and reachable at a predictable location.

### Aggregation & Inconsistency
To demonstrate the challenges of distributed consistency as required by the assignment:
* **Task 7 (READ_SCORE)** and **Task 8 (READ_ALL)** are executed on a **randomly selected fragment**.
* This approach results in a partial view of the global data state, leading to the expected accuracy variance (approx. 30-40% accuracy) when compared to a single-instance baseline.

## How to Run
1. Ensure you have $N$ PostgreSQL databases created (e.g., `fragment0`, `fragment1`, `fragment2`).
2. Run the `script.sql` on each fragment to initialize schemas.
3. Compile the project using Maven:
   ```bash
   mvn clean install
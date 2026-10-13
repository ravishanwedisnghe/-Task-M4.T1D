#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
command -v mpic++ >/dev/null || { echo "MPI compiler missing. Install Open MPI first."; exit 1; }
mpic++ -std=c++17 -O3 -Wall -Wextra -pthread traffic_mpi_original.cpp -o traffic_mpi_original
mpic++ -std=c++17 -O3 -Wall -Wextra -pthread traffic_mpi_optimised.cpp -o traffic_mpi_optimised
c++ -std=c++17 -O3 -Wall -Wextra -pthread traffic_sim.cpp -o traffic_sim
c++ -std=c++17 -O3 -Wall -Wextra generate_data.cpp -o generate_data
mkdir -p results data
echo "Build complete. Run: python3 test_project.py"

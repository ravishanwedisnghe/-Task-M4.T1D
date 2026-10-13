#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p results
{
  date
  sw_vers
  uname -m
  sysctl -n machdep.cpu.brand_string
  sysctl hw.physicalcpu hw.logicalcpu hw.memsize
  mpirun --version
  mpic++ --version
} 2>&1 | tee results/machine_specs.txt

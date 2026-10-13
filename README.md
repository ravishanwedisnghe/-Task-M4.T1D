# SIT315 M4.T1D – Traffic Analysis Using MPI

This project extends my previous traffic simulator. It uses MPI processes and producer and consumer threads to find the busiest traffic lights for each hour.

## New Features

- Binary messages for MPI data exchange.
- Distributed top N candidate selection.
- Stage timings and communication counts.
- Stronger correctness tests.

## Requirements

- C++17 compiler
- MPI
- Python 3

The project was tested on an Apple M4 Mac with 16 GiB RAM.

## Build and Run

Compile all programs:

    bash build.sh

Run the correctness tests:

    python3 test_project.py

Run the small demonstration:

    mpirun --bind-to none -np 2 ./traffic_mpi_optimised data/traffic_demo.csv 3 1 1 1024

## Results

All correctness tests passed. The improved MPI version was faster than the original MPI version in the measured comparisons. Sequential processing remained faster on this Mac.

The report explains the design, tests, performance results, and stress tests. The video links PDF contains the demonstration links.

#!/bin/env bash

yarn jar zad7-all.jar Benchmark 1 3 128 > bench_1_3_128.txt 2>&1

yarn jar zad7-all.jar Benchmark 1 1 128 > bench_1_1_128.txt 2>&1
yarn jar zad7-all.jar Benchmark 1 2 128 > bench_1_2_128.txt 2>&1

yarn jar zad7-all.jar Benchmark 2 3 128 > bench_2_3_128.txt 2>&1
yarn jar zad7-all.jar Benchmark 3 3 128 > bench_3_3_128.txt 2>&1

yarn jar zad7-all.jar Benchmark 1 3 32 > bench_1_3_32.txt 2>&1
yarn jar zad7-all.jar Benchmark 1 3 64 > bench_1_3_64.txt 2>&1
yarn jar zad7-all.jar Benchmark 1 3 192 > bench_1_3_192.txt 2>&1
yarn jar zad7-all.jar Benchmark 1 3 256 > bench_1_3_256.txt 2>&1

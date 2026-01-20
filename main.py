#!/usr/bin/env python3
"""
RGW Bucket Stats Collector - Main entry point.

Usage:
    python main.py --db stats.duckdb collect
    python main.py --db stats.duckdb collect --continuous
    python main.py --db stats.duckdb status
    python main.py --db stats.duckdb query --type top-buckets
"""

from src.cli import main

if __name__ == '__main__':
    main()

#!/bin/bash

# Test runner script for binfiles project
# Usage: ./run_tests.sh [unit|functional|performance|all]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run unit tests
run_unit_tests() {
    print_status "Running unit tests..."
    cd binfile/test/unit
    go test -v -race -coverprofile=coverage.out .
    if [ $? -eq 0 ]; then
        print_success "Unit tests passed"
        go tool cover -html=coverage.out -o coverage.html
        print_status "Coverage report generated: coverage.html"
    else
        print_error "Unit tests failed"
        return 1
    fi
    cd ../../..
}

# Function to run functional tests
run_functional_tests() {
    print_status "Running functional tests..."
    cd binfile/test/functional
    go test -v -timeout=10m .
    if [ $? -eq 0 ]; then
        print_success "Functional tests passed"
    else
        print_error "Functional tests failed"
        return 1
    fi
    cd ../../..
}

# Function to run performance tests
run_performance_tests() {
    print_status "Running performance tests..."
    cd binfile/test/performance
    go test -v -bench=. -benchmem .
    if [ $? -eq 0 ]; then
        print_success "Performance tests passed"
    else
        print_error "Performance tests failed"
        return 1
    fi
    cd ../../..
}

# Function to run all tests
run_all_tests() {
    print_status "Running all tests..."
    
    run_unit_tests
    if [ $? -ne 0 ]; then
        print_error "Unit tests failed, stopping"
        exit 1
    fi
    
    run_functional_tests
    if [ $? -ne 0 ]; then
        print_error "Functional tests failed, stopping"
        exit 1
    fi
    
    run_performance_tests
    if [ $? -ne 0 ]; then
        print_error "Performance tests failed, stopping"
        exit 1
    fi
    
    print_success "All tests passed!"
}

# Function to show help
show_help() {
    echo "Usage: $0 [unit|functional|performance|all]"
    echo ""
    echo "Commands:"
    echo "  unit        Run unit tests only"
    echo "  functional  Run functional tests only"
    echo "  performance Run performance tests only"
    echo "  all         Run all tests (default)"
    echo "  help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 unit"
    echo "  $0 functional"
    echo "  $0 performance"
    echo "  $0 all"
}

# Main script logic
case "${1:-all}" in
    unit)
        run_unit_tests
        ;;
    functional)
        run_functional_tests
        ;;
    performance)
        run_performance_tests
        ;;
    all)
        run_all_tests
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac

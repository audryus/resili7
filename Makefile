
# Run all tests in the repository once to ensure correctness.
# Using -count=1 to bypass test caching and get fresh results.
test:
	go test -count=1 -v ./...
	
# Analyzes memory allocation and heap escapes.
# This is crucial to verify our "zero-allocation" goal in the hot-path.
# We use double -m flags for detailed escape analysis output.
check-escape:
	go build -gcflags="-m -m" ./... 2>&1 | grep -v "skip"

# Executes CPU profiling during the Full Stack Benchmark.
# This identifies bottlenecks, contention, and areas for optimization.
# Outputs a 'cpu.pprof' file for further analysis.
profile:
	go test -bench=BenchmarkHttpFullStack -cpuprofile http_cpu.pprof ./
	go test -bench=BenchmarkGrpcFullStack -cpuprofile grpc_cpu.pprof ./
	go test -bench=BenchmarkWsFullStack -cpuprofile ws_cpu.pprof ./
	@echo "Profile generated in cpu.pprof. Use 'make profile-web' for visual analysis."

# Launches the pprof web interface on port 8080.
# Requires Graphviz to be installed for visual graph generation.
profile-web:
	go tool pprof -http=:8080 cpu.pprof

# Cleans test caches and removes profiling artifacts.
clean:
	go clean -testcache
	rm -f cpu.pprof
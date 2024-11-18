# Makefile for compiling the CSV to HTY converter and analysis with nlohmann/json

# Compiler
CXX = g++
CXXFLAGS = -std=c++20 -Wall

# Directories
BIN_DIR = bin

# Target: convert
convert: src/csv_to_hty.cpp
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/convert.out src/csv_to_hty.cpp -ljsoncpp

# Target: analyze
analyze: src/analyze.cpp
	$(CXX) $(CXXFLAGS) -o $(BIN_DIR)/analyze.out src/analyze.cpp -I../third_party

# Clean build artifacts
clean:
	rm -f $(BIN_DIR)/*.out

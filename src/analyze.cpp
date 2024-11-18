#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

// Function to swap endianness if needed
int32_t swap_endian(int32_t value) {
    return ((value & 0xFF000000) >> 24) |
           ((value & 0x00FF0000) >> 8) |
           ((value & 0x0000FF00) << 8) |
           ((value & 0x000000FF) << 24);
}

// Function to swap endianness for float
float swap_endian_float(float value) {
    int32_t temp = *reinterpret_cast<int32_t*>(&value);
    temp = swap_endian(temp);
    return *reinterpret_cast<float*>(&temp);
}

nlohmann::json extract_metadata(const std::string& hty_file_path) {
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        std::cerr << "Error: Unable to open HTY file: " << hty_file_path << "\n";
        throw std::runtime_error("File not found or could not be opened.");
    }

    // Seek to the end of the file to read the last 4 bytes for metadata size
    hty_file.seekg(-static_cast<std::streamoff>(sizeof(int32_t)), std::ios::end);
    if (hty_file.fail()) {
        std::cerr << "Error: Failed to seek to the metadata size position.\n";
        throw std::runtime_error("Failed to seek to metadata size.");
    }

    int32_t metadata_size;
    hty_file.read(reinterpret_cast<char*>(&metadata_size), sizeof(metadata_size));
    if (hty_file.fail()) {
        std::cerr << "Error: Did not read metadata size correctly.\n";
        throw std::runtime_error("Failed to read metadata size.");
    }

    // Handle potential endianness of the metadata_size if necessary
    metadata_size = swap_endian(metadata_size);  // Ensure this swap_endian function works correctly
    std::cout << "Metadata size read: " << metadata_size << std::endl;

    // Seek to the position of the metadata string
    hty_file.seekg(-static_cast<std::streamoff>(metadata_size + sizeof(int32_t)), std::ios::end);
    if (hty_file.fail()) {
        std::cerr << "Error: Failed to seek to the metadata position.\n";
        throw std::runtime_error("Failed to seek to metadata.");
    }

    // Read the metadata string
    std::string metadata_str(metadata_size, '\0');
    hty_file.read(&metadata_str[0], metadata_size);
    if (hty_file.fail()) {
        std::cerr << "Error: Did not read metadata string correctly.\n";
        throw std::runtime_error("Failed to read metadata.");
    }

    hty_file.close();  // Close the file

    // Parse the JSON string into a nlohmann::json object
    try {
        nlohmann::json metadata = nlohmann::json::parse(metadata_str);
        return metadata;
    } catch (const nlohmann::json::parse_error& e) {
        std::cerr << "Error: Failed to parse metadata JSON: " << e.what() << std::endl;
        throw std::runtime_error("Failed to parse metadata.");
    }
}


// Function to swap endianness for integers
int32_t swap_endian_int(int32_t value) {
    return ((value & 0xFF000000) >> 24) |
           ((value & 0x00FF0000) >> 8) |
           ((value & 0x0000FF00) << 8) |
           ((value & 0x000000FF) << 24);
}


std::vector<int> project_single_column(nlohmann::json metadata, std::string hty_file_path, std::string projected_column) {
    std::vector<int> column_data;
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        throw std::runtime_error("File not found or could not be opened.");
    }

    // Get the number of groups and rows from metadata
    int num_groups = metadata["num_groups"];
    int num_rows = metadata["num_rows"];

    // Iterate through each group to find the requested column
    for (int i = 0; i < num_groups; ++i) {
        auto group = metadata["groups"][i];
        int num_columns = group["num_columns"];
        int offset = group["offset"]; // Get the starting offset of this group

        // Iterate through each column in the current group
        for (int j = 0; j < num_columns; ++j) {
            auto column = group["columns"][j];
            std::string column_name = column["column_name"];
            std::string column_type = column["column_type"];

            // Check if the current column matches the projected column
            if (column_name == projected_column) {
                std::cout << "Found column: " << projected_column << " at offset: " << offset << std::endl;

                // Calculate the size of each row based on the number of columns
                int row_size = 0;
                for (int k = 0; k < num_columns; ++k) {
                    std::string type = group["columns"][k]["column_type"];
                    if (type == "int") {
                        row_size += sizeof(int32_t);
                    } else if (type == "float") {
                        row_size += sizeof(float);
                    }
                }

                std::cout << "Calculated row size: " << row_size << " bytes" << std::endl;

                // Now read the data for the specific column
                for (int k = 0; k < num_rows; ++k) {
                    // Calculate the current offset for the column value
                    int current_offset = offset + k * row_size + (j * (column_type == "float" ? sizeof(float) : sizeof(int32_t)));
                    hty_file.seekg(current_offset, std::ios::beg);

                    if (column_type == "int") {
                        int32_t value;
                        hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                        if (hty_file.fail()) {
                            throw std::runtime_error("Failed to read integer data.");
                        }
                        // Swap endianness if necessary
                        value = swap_endian_int(value);

                    } else if (column_type == "float") {
                        float value;
                        hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                        if (hty_file.fail()) {
                            throw std::runtime_error("Failed to read float data.");
                        }
                        value = swap_endian_float(value); // Swap endianness if needed
                        column_data.push_back(static_cast<int>(value)); // Cast to int if needed
                    }
                }
                break; // Exit the column loop once we find the column
            }
        }
    }

    hty_file.close();
    return column_data;
}

std::vector<int> filter(nlohmann::json metadata, std::string hty_file_path, std::string projected_column, int operation, int filtered_value) {
    std::vector<int> filtered_data;
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        throw std::runtime_error("File not found or could not be opened.");
    }

    // Find the number of rows
    int num_rows = metadata["num_rows"];

    // Loop through the groups in the metadata
    for (const auto& group : metadata["groups"]) {
        int num_columns = group["num_columns"];
        int offset = group["offset"];

        // Initialize variables to track column index and type
        int column_offset = -1;
        std::string column_type;
        int column_byte_offset = 0;

        // Find the target column index and its type
        for (int j = 0; j < num_columns; ++j) {
            auto column = group["columns"][j];
            std::string column_name = column["column_name"];
            column_type = column["column_type"];

            if (column_name == projected_column) {
                column_offset = j;
                break;
            }
            column_byte_offset += (column["column_type"] == "float") ? sizeof(float) : sizeof(int32_t);
        }

        if (column_offset == -1) continue;  // Skip if the column is not found

        int row_size = column_byte_offset + ((column_type == "float") ? sizeof(float) : sizeof(int32_t));
        // Now read the data for filtering
        for (int i = 0; i < num_rows; ++i) {
            int current_offset = offset + i * row_size + column_byte_offset;
            hty_file.seekg(current_offset, std::ios::beg);

            bool condition_met = false;
            if (column_type == "int") {
                int32_t value;
                hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                value = swap_endian_int(value);

                switch (operation) {
                    case 0: condition_met = (value == filtered_value); break;
                    case 1: condition_met = (value != filtered_value); break;
                    case 2: condition_met = (value > filtered_value); break;
                    case 3: condition_met = (value >= filtered_value); break;
                    case 4: condition_met = (value < filtered_value); break;
                    case 5: condition_met = (value <= filtered_value); break;
                }

                if (condition_met) filtered_data.push_back(value);
            } 
            else if (column_type == "float") {
                float value;
                hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                value = swap_endian_float(value);
                
                int int_value = static_cast<int>(value);
                // Debug output for the read value

                switch (operation) {
                    case 0: condition_met = (int_value == filtered_value); break;
                    case 1: condition_met = (int_value != filtered_value); break;
                    case 2: condition_met = (int_value > filtered_value); break;
                    case 3: condition_met = (int_value >= filtered_value); break;
                    case 4: condition_met = (int_value < filtered_value); break;
                    case 5: condition_met = (int_value <= filtered_value); break;
                }
                // Print filtering condition results


                if (condition_met) filtered_data.push_back(int_value);
            }
        }
    }

    hty_file.close();
    return filtered_data;
}

std::vector<std::vector<int>> project(nlohmann::json metadata, std::string hty_file_path, std::vector<std::string> projected_columns) {
    std::vector<std::vector<int>> projected_data;
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        throw std::runtime_error("File not found or could not be opened.");
    }

    // Get the number of rows
    int num_rows = metadata["num_rows"];
    
    // Initialize the projected data structure first
    projected_data.resize(projected_columns.size(), std::vector<int>(num_rows));

    // Find the group containing all columns and calculate offsets
    for (const auto& group : metadata["groups"]) {
        int group_offset = group["offset"];
        
        // Calculate total row size for this group
        int row_size = 0;
        for (const auto& col : group["columns"]) {
            std::string type = col["column_type"];
            row_size += (type == "int") ? sizeof(int32_t) : sizeof(float);
        }

        // Process each requested column
        for (size_t proj_idx = 0; proj_idx < projected_columns.size(); ++proj_idx) {
            const std::string& target_column = projected_columns[proj_idx];
            
            // Find column offset within the group
            int column_byte_offset = 0;
            bool column_found = false;
            std::string column_type;

            for (const auto& col : group["columns"]) {
                if (col["column_name"] == target_column) {
                    column_found = true;
                    column_type = col["column_type"];
                    break;
                }
                column_byte_offset += (col["column_type"] == "int") ? sizeof(int32_t) : sizeof(float);
            }

            if (!column_found) continue;

            // Read data for this column
            for (int row = 0; row < num_rows; ++row) {
                int current_offset = group_offset + (row * row_size) + column_byte_offset;
                hty_file.seekg(current_offset, std::ios::beg);

                if (column_type == "int") {
                    int32_t value;
                    hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                    value = swap_endian_int(value);
                    projected_data[proj_idx][row] = value;
                } else if (column_type == "float") {
                    float value;
                    hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                    value = swap_endian_float(value);
                    projected_data[proj_idx][row] = static_cast<int>(value);
                }

                if (hty_file.fail()) {
                    throw std::runtime_error("Failed to read data at offset " + std::to_string(current_offset));
                }
            }
        }
    }

    hty_file.close();

    // Print the results in CSV format
    // Print header
    for (size_t i = 0; i < projected_columns.size(); ++i) {
        std::cout << projected_columns[i];
        if (i < projected_columns.size() - 1) {
            std::cout << ",";
        }
    }
    std::cout << std::endl;
    

    // Print data rows
    for (int row = 0; row < num_rows; ++row) {
        for (size_t col = 0; col < projected_columns.size(); ++col) {
            std::cout << projected_data[col][row];
            if (col < projected_columns.size() - 1) {
                std::cout << ",";
            }
        }
        std::cout << std::endl;
    }

    return projected_data;
}

std::vector<std::vector<int>> project_and_filter(nlohmann::json metadata, std::string hty_file_path, 
    std::vector<std::string> projected_columns, std::string filtered_column, int op, int value) {
    
    std::vector<std::vector<int>> filtered_data;
    std::ifstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        throw std::runtime_error("File not found or could not be opened.");
    }

    nlohmann::json target_group;
    
    // Add filtered_column to the list of columns we need to find
    std::vector<std::string> all_needed_columns = projected_columns;
    all_needed_columns.push_back(filtered_column);

    // Find the group containing all needed columns
    for (const auto& group : metadata["groups"]) {
        bool all_columns_found = true;
        for (const auto& needed_col : all_needed_columns) {
            bool found = false;
            for (const auto& col : group["columns"]) {
                if (col["column_name"] == needed_col) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                all_columns_found = false;
                break;
            }
        }
        if (all_columns_found) {
            target_group = group;
            break;
        }
    }

    if (target_group.empty()) {
        throw std::runtime_error("Not all columns are in the same group");
    }

    // Get basic information
    int num_rows = metadata["num_rows"];
    int group_offset = target_group["offset"];

    // Calculate row size and column offsets
    int row_size = 0;
    std::vector<std::pair<int, std::string>> column_offsets;  // offset and type for projected columns
    int filter_column_offset = -1;
    std::string filter_column_type;

    // First pass: calculate row size and store column offsets
    int current_offset = 0;
    for (const auto& col : target_group["columns"]) {
        std::string col_name = col["column_name"];
        std::string col_type = col["column_type"];
        int col_size = (col_type == "int") ? sizeof(int32_t) : sizeof(float);

        // Store offset for projected columns
        for (const auto& proj_col : projected_columns) {
            if (col_name == proj_col) {
                column_offsets.push_back({current_offset, col_type});
            }
        }

        // Store offset for filter column
        if (col_name == filtered_column) {
            filter_column_offset = current_offset;
            filter_column_type = col_type;
        }

        current_offset += col_size;
        row_size += col_size;
    }

    // Initialize result vectors
    std::vector<std::vector<int>> result(projected_columns.size());

    // Read and filter data
    for (int row = 0; row < num_rows; ++row) {
        // First read and check the filter column
        int row_offset = group_offset + (row * row_size);
        hty_file.seekg(row_offset + filter_column_offset, std::ios::beg);

        bool include_row = false;
        if (filter_column_type == "int") {
            int32_t filter_value;
            hty_file.read(reinterpret_cast<char*>(&filter_value), sizeof(filter_value));
            filter_value = swap_endian_int(filter_value);

            switch (op) {
                case 0: include_row = (filter_value == value); break;
                case 1: include_row = (filter_value != value); break;
                case 2: include_row = (filter_value > value); break;
                case 3: include_row = (filter_value >= value); break;
                case 4: include_row = (filter_value < value); break;
                case 5: include_row = (filter_value <= value); break;
            }
        } else { // float
            float filter_value;
            hty_file.read(reinterpret_cast<char*>(&filter_value), sizeof(filter_value));
            filter_value = swap_endian_float(filter_value);
            int int_filter_value = static_cast<int>(filter_value);

            switch (op) {
                case 0: include_row = (int_filter_value == value); break;
                case 1: include_row = (int_filter_value != value); break;
                case 2: include_row = (int_filter_value > value); break;
                case 3: include_row = (int_filter_value >= value); break;
                case 4: include_row = (int_filter_value < value); break;
                case 5: include_row = (int_filter_value <= value); break;
            }
        }

        // If row passes filter, read projected columns
        if (include_row) {
            for (size_t i = 0; i < column_offsets.size(); ++i) {
                hty_file.seekg(row_offset + column_offsets[i].first, std::ios::beg);
                
                if (column_offsets[i].second == "int") {
                    int32_t value;
                    hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                    value = swap_endian_int(value);
                    result[i].push_back(value);
                } else { // float
                    float value;
                    hty_file.read(reinterpret_cast<char*>(&value), sizeof(value));
                    value = swap_endian_float(value);
                    result[i].push_back(static_cast<int>(value));
                }
            }
        }
    }

    hty_file.close();

    // Print results in CSV format
    // Print header
    for (size_t i = 0; i < projected_columns.size(); ++i) {
        std::cout << projected_columns[i];
        if (i < projected_columns.size() - 1) {
            std::cout << ",";
        }
    }
    std::cout << std::endl;

    // Print data rows
    for (size_t row = 0; row < result[0].size(); ++row) {
        for (size_t col = 0; col < result.size(); ++col) {
            std::cout << result[col][row];
            if (col < result.size() - 1) {
                std::cout << ",";
            }
        }
        std::cout << std::endl;
    }

    return result;
}


// Function to display the projected column data
void display_column(nlohmann::json metadata, std::string column_name, std::vector<int> data) {
    std::cout << "display" << std::endl;
    std::cout << column_name << std::endl; // Display the column name
    for (const auto& value : data) {
        std::cout << value << std::endl; // Display each value in the column
    }
}

void display_menu() {
    std::cout << "1. Display column data\n";
    std::cout << "2. Filter column data\n";
    std::cout << "3. Project columns\n";
    std::cout << "4. Project and filter columns\n";
    std::cout << "5. Add rows to the HTY file\n";  
    std::cout << "6. Exit\n";
}


void display_column_data(const std::string& hty_file_path, const std::string& column_name, nlohmann::json& metadata) {
    // Implement the display column functionality
    std::vector<int> column_data = project_single_column(metadata, hty_file_path, column_name);
    display_column(metadata, column_name, column_data);
}

std::vector<std::string> get_projected_columns() {
    std::vector<std::string> columns;
    std::string input;

    std::cout << "Enter the column names to project (separated by spaces, type 'done' when finished): ";

    while (true) {
        std::cin >> input;
        if (input == "done") { // Check 'done' before pushing to columns
            break;
        }
        columns.push_back(input);
    }

    // Debugging output to print the columns entered by the user
    std::cout << "Columns entered: ";
    for (const auto& col : columns) {
        std::cout << col << " ";
    }
    std::cout << std::endl;

    return columns;
}

void add_row(nlohmann::json metadata, const std::string& hty_file_path, const std::string& modified_hty_file_path, const std::vector<std::vector<int>>& rows) {
    std::ifstream in_file(hty_file_path, std::ios::binary);
    if (!in_file.is_open()) {
        std::cerr << "Error: Unable to open HTY file for reading.\n";
        return;
    }

    // Read the entire file content
    std::vector<char> file_content((std::istreambuf_iterator<char>(in_file)), std::istreambuf_iterator<char>());
    in_file.close();

    // Update the metadata with the new number of rows
    metadata["num_rows"] = metadata["num_rows"].get<int>() + rows.size();

    // Write the updated metadata to a string
    std::string metadata_str = metadata.dump();
    int32_t metadata_size = swap_endian_int(metadata_str.size());

    // Open the modified HTY file for writing
    std::ofstream out_file(modified_hty_file_path, std::ios::binary);
    if (!out_file.is_open()) {
        std::cerr << "Error: Unable to open modified HTY file for writing.\n";
        return;
    }

    // Write the original content to the modified file excluding the old metadata and its size
    out_file.write(file_content.data(), file_content.size() - sizeof(int32_t) - swap_endian_int(*reinterpret_cast<int32_t*>(&file_content[file_content.size() - sizeof(int32_t)])));

    // Write the new rows to the modified file
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            const auto& value = row[i];
            std::string column_type = metadata["groups"][0]["columns"][i]["column_type"];
            if (column_type == "int") {
                int32_t int_value = swap_endian_int(value);
                out_file.write(reinterpret_cast<const char*>(&int_value), sizeof(int32_t));
            } else if (column_type == "float") {
                float float_value = static_cast<float>(value);
                float_value = swap_endian_float(float_value);
                out_file.write(reinterpret_cast<const char*>(&float_value), sizeof(float));
            }
        }
    }

    // Write the updated metadata to the modified file
    out_file.write(metadata_str.c_str(), metadata_str.size());
    out_file.write(reinterpret_cast<const char*>(&metadata_size), sizeof(metadata_size));

    out_file.close();
}


int main() {
    std::string hty_file_path = "src/output.hty"; // Path to your HTY file
    nlohmann::json metadata;

    // Extract the metadata first
    try {
        metadata = extract_metadata(hty_file_path);
    } catch (const std::exception& e) {
        std::cerr << "An error occurred while reading metadata: " << e.what() << std::endl;
        return 1;
    }

    int choice = 0;
    while (choice != 6) { // Change exit choice to 6
        display_menu();
        std::cin >> choice;

        switch (choice) {
            case 1: {
                std::string column_name;
                std::cout << "Enter the column name to display: ";
                std::cin >> column_name;
                display_column_data(hty_file_path, column_name, metadata);
                break;
            }
            case 2: {
                std::string column_name;
                int operation;
                int filtered_value;
                
                std::cout << "Enter the column name to filter: ";
                std::cin >> column_name;    
                std::cout << "Choose an operation (0: =, 1: !=, 2: >, 3: >=, 4: <, 5: <=): ";
                std::cin >> operation;
                std::cout << "Enter the value to filter: ";
                std::cin >> filtered_value;

                // Call the filter function
                std::vector<int> filtered_data = filter(metadata, hty_file_path, column_name, operation, filtered_value);
                
                // Display the filtered results
                std::cout << "Filtered results for column " << column_name << ":\n";
                for (const auto& value : filtered_data) {
                    std::cout << value << std::endl;
                }
                break;
            }
            case 3: {
                // Get the projected columns from user input
                std::vector<std::string> projected_columns = get_projected_columns();
                // Call the project function
                std::vector<std::vector<int>> projected_data = project(metadata, hty_file_path, projected_columns);
                break;
            }
            case 4: {
                std::vector<std::string> projected_columns = get_projected_columns();
                
                std::string filtered_column;
                int operation;
                int filtered_value;
                
                std::cout << "Enter the column name to filter on: ";
                std::cin >> filtered_column;
                
                std::cout << "Choose an operation (0: =, 1: !=, 2: >, 3: >=, 4: <, 5: <=): ";
                std::cin >> operation;
                
                std::cout << "Enter the value to filter by: ";
                std::cin >> filtered_value;
                
                // Call the project_and_filter function
                std::vector<std::vector<int>> result = project_and_filter(
                    metadata, hty_file_path, projected_columns, filtered_column, operation, filtered_value
                );
                break;
            }
            case 5: {
                // Ask for the modified HTY file path
                std::string modified_hty_file_path = "src/modified_output.hty";
                // Ask for the number of rows to add
                int num_rows;
                std::cout << "Enter the number of rows to add: ";
                std::cin >> num_rows;

                // Collect the rows of data
                std::vector<std::vector<int>> rows;
                for (int i = 0; i < num_rows; ++i) {
                    std::cout << "Enter data for row " << i + 1 << ":\n";
                    std::vector<int> row_data;

                    // Assuming the number of columns is known or can be inferred
                    int num_columns = 3; // Change this to match the number of columns in your file

                    for (int j = 0; j < num_columns; ++j) {
                        int value;
                        std::cout << "Enter value for column " << j + 1 << ": ";
                        std::cin >> value;
                        row_data.push_back(value);
                    }

                    rows.push_back(row_data);
                }

                // Call the add_row function with the collected rows
                add_row(metadata, hty_file_path, modified_hty_file_path, rows);
                break;
            }
            case 6:
                std::cout << "Exiting...\n";
                break;
            default:
                std::cout << "Invalid choice. Please try again.\n";
                break;
        }
    }

    return 0;
}

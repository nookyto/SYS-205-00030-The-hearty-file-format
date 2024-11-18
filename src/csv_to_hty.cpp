#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <jsoncpp/json/json.h>

// Helper functions to convert data to binary in big-endian format
void write_int32(std::ofstream& ofs, int32_t value) {
    // Convert to big-endian format
    uint8_t bytes[4];
    bytes[0] = (value >> 24) & 0xFF; // Most significant byte
    bytes[1] = (value >> 16) & 0xFF;
    bytes[2] = (value >> 8) & 0xFF;
    bytes[3] = value & 0xFF;         // Least significant byte
    ofs.write(reinterpret_cast<const char*>(bytes), sizeof(bytes));
}

void write_float32(std::ofstream& ofs, float value) {
    // Convert to integer representation first
    int32_t int_value = *reinterpret_cast<int32_t*>(&value);
    
    // Then write it as big-endian
    write_int32(ofs, int_value);
}

void convert_from_csv_to_hty(std::string csv_file_path, std::string hty_file_path) {
    std::ifstream csv_file(csv_file_path);
    if (!csv_file.is_open()) {
        std::cerr << "Error: Unable to open CSV file.\n";
        return;
    }

    std::vector<std::vector<std::string>> csv_data;
    std::string line, word;
    while (std::getline(csv_file, line)) {
        std::stringstream ss(line);
        std::vector<std::string> row;
        while (std::getline(ss, word, ',')) {
            row.push_back(word);
        }
        csv_data.push_back(row);
    }
    csv_file.close();

    int num_rows = csv_data.size();
    int num_columns = csv_data[0].size();

    std::ofstream hty_file(hty_file_path, std::ios::binary);
    if (!hty_file.is_open()) {
        std::cerr << "Error: Unable to open HTY file.\n";
        return;
    }

    // Writing raw data
    for (const auto& row : csv_data) {
        for (int i = 0; i < num_columns; ++i) {
            if (i == 2) {  // Assuming the 3rd column is float
                float float_value = std::stof(row[i]);
                write_float32(hty_file, float_value);
            } else {
                int32_t int_value = std::stoi(row[i]);
                write_int32(hty_file, int_value);
            }
        }
    }

    // Prepare the metadata
    Json::Value metadata;
    metadata["num_rows"] = num_rows;
    metadata["num_groups"] = 1;

    Json::Value group;
    group["num_columns"] = num_columns;
    group["offset"] = 0;

    Json::Value columns(Json::arrayValue);
    Json::Value column1;
    column1["column_name"] = "id";
    column1["column_type"] = "int";
    columns.append(column1);

    Json::Value column2;
    column2["column_name"] = "type";
    column2["column_type"] = "int";
    columns.append(column2);

    Json::Value column3;
    column3["column_name"] = "salary";
    column3["column_type"] = "float";
    columns.append(column3);

    group["columns"] = columns;
    metadata["groups"].append(group);

    Json::StreamWriterBuilder writer;
    std::string metadata_str = Json::writeString(writer, metadata);

    // Write the metadata string to the file first
    hty_file.write(metadata_str.c_str(), metadata_str.size());

    // Now write the metadata size after the metadata has been written
    int32_t metadata_size = static_cast<int32_t>(metadata_str.size());
    write_int32(hty_file, metadata_size);

    hty_file.close();
}

int main() {
    std::string csv_file_path = "src/data.csv";
    std::string hty_file_path = "src/output.hty";
    convert_from_csv_to_hty(csv_file_path, hty_file_path);
    return 0;
}

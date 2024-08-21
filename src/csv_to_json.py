import os
import json

def get_file_info(directory):
    file_info = {}
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_info[filename] = 'cc' in filename
    return file_info

def save_to_json(data, output_file):
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=4)

if __name__ == "__main__":
    directory = 'data'  
    output_file = 'data_paths.json'
    file_info = get_file_info(directory)
    save_to_json(file_info, output_file)
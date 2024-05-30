import os


def split_log_file(input_file, output_dir, num_files):
    # Open the input file and read all lines
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Calculate the number of lines per file
    lines_per_file = len(lines) // num_files

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Write chunks of lines to separate files
    for i in range(num_files):
        start_index = i * lines_per_file
        end_index = start_index + lines_per_file if i < num_files - 1 else None

        with open(os.path.join(output_dir, f'output_{i + 1}.txt'), 'w') as file:
            file.writelines(lines[start_index:end_index])

    print("Log file split successfully!")


# Example usage:
split_log_file("logs_oem_data_2023_to_2013_missing_files", "output_dir", 22)

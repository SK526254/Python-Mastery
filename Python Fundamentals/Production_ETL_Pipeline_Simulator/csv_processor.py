from pathlib import Path

input_dir =  Path.cwd() / 'input'
file_count = 0
for file in input_dir.iterdir():
    if file.is_file() and file.name.startswith('transaction') and file.suffix == '.csv':
        file_count += 1
# print(input_dir)



print(f'Total transaction files found: {file_count}')
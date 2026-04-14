
import os

FILE_PATH = "/home/chungnt/crawlvip/craw/dashboard.py"
START_LINE = 2491 # 1-indexed, inclusive
END_LINE_EXCLUSIVE = 3486 # 1-indexed (This is 'with tab3:')

def indent_file():
    with open(FILE_PATH, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    new_lines = []
    for i, line in enumerate(lines):
        line_num = i + 1
        if START_LINE <= line_num < END_LINE_EXCLUSIVE:
            # Indent if not empty line (or indent empty lines too, doesn't matter for python usually, but better to check)
            if line.strip():
                new_lines.append("    " + line)
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
            
    with open(FILE_PATH, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)
    print(f"Indented lines {START_LINE} to {END_LINE_EXCLUSIVE}")

if __name__ == "__main__":
    indent_file()

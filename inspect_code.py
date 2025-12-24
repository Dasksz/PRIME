
with open('index.html', 'r') as f:
    lines = f.readlines()

# Read Coverage View HTML (approx 1570 to 1650)
start_line = 1570
end_line = 1650
print(f"--- Lines {start_line}-{end_line} ---")
print("".join(lines[start_line:end_line]))

# Search for JS populating growth-table-body
import re
for i, line in enumerate(lines):
    if "getElementById('growth-table-body')" in line:
        print(f"\n--- Found growth-table-body JS at line {i} ---")
        # Print context (function definition and inner loop)
        # It's likely inside renderStockAnalysisTable or similar
        print("".join(lines[i-20:i+100]))
        break

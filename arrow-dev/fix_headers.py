"""
Fix the Dlang headers generated for the Arrow project by `Dpp`.
"""

import re
import sys
from typing import List


def fix_headers(input: List[str]) -> List[str]:
    pattern = re.compile(r"\s*struct \w+;")
    output = []
    for line in input:
        if re.match(pattern, line):
            output.append(line.rstrip(";") + "{};")
        else:
            output.append(line)
    return output


if __name__ == "__main__":
    with open(sys.argv[1], 'r') as f:
        output = fix_headers(f.read().split("\n"))

    with open(sys.argv[2], 'w') as f:
        f.write("module darrow.arrow_bindings;\n")
        for line in output:
            f.write(line + "\n")

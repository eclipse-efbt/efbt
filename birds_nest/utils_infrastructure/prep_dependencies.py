import re

with open('requirements.txt', 'r') as f:
    with open('new_req.txt', "w") as f_w:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Parse package==version format
                match = re.match(r'^([^=<>!]+)==([^=<>!]+)$', line)
                if match:
                    name = match.group(1).lower().replace('_', '-')
                    version = match.group(2)
                    f_w.write(f'pypi/pypi/-/{name}/{version}')
                    f_w.write('\n')

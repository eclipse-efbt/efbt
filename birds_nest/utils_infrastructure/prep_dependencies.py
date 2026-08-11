import re

with open('requirements.txt', 'r') as f:
    with open('new_req.txt', "w") as f_w:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                # Drop any environment marker ("package==1.0 ; sys_platform ==
                # 'win32'") so that conditional dependencies are still scanned.
                line = line.split(';')[0].strip()
                # Parse package==version format
                match = re.match(r'^([^=<>!]+)==([^=<>!]+)$', line)
                if match:
                    # ClearlyDefined and the Eclipse license service both key on
                    # the PEP 503 normalized name, which collapses any run of
                    # "-", "_" or "." to a single "-".
                    name = re.sub(r'[-_.]+', '-', match.group(1)).lower()
                    version = match.group(2)
                    f_w.write(f'pypi/pypi/-/{name}/{version}')
                    f_w.write('\n')

import json
import os
import subprocess

## Validate

# gtfs-validator
subprocess.run(["java", "-jar", "../misc/tools/gtfs-validator/gtfs-validator-4.1.0-cli.jar", "-i", "../processing/bmtc.zip", "-o", "output/gtfs-validator"])

# gtfsvtor
subprocess.run(["../misc/tools/gtfsvtor/bin/gtfsvtor", "../processing/bmtc.zip", "-o", "output/gtfsvtor/validation.html"])

# transport-validator
f = open("output/transport-validator/validation.json", "w")
subprocess.run(["cargo", "run", "--release", "--manifest-path", "../misc/tools/transport-validator/Cargo.toml", "--", "--input", "../processing/bmtc.zip"], stdout = f)
f.close()

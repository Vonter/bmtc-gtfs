import subprocess

# gtfs-validator
subprocess.run(["java", "-jar", "../tools/gtfs-validator/gtfs-validator-7.0.0-cli.jar", "-i", "../gtfs/bmtc.zip", "-o", "../validation/gtfs-validator"])

# gtfsvtor
subprocess.run(["../tools/gtfsvtor/bin/gtfsvtor", "../gtfs/bmtc.zip", "-o", "../validation/gtfsvtor/validation.html"])

# transport-validator
f = open("../validation/transport-validator/validation.json", "w")
subprocess.run(["cargo", "run", "--release", "--manifest-path", "../tools/transport-validator/Cargo.toml", "--", "--input", "../gtfs/bmtc.zip"], stdout = f)
f.close()

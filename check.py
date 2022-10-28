import subprocess
import tempfile

with tempfile.TemporaryFile() as tempf:
    proc = subprocess.Popen(['aws', 's3', 'ls',
                             's3://AF_data/true_structure_dataset/msa/msa_0/'
                             '--recursive',
                             '--human-readable',
                             '--summarize'], stdout=tempf)
    proc.wait()
    tempf.seek(0)
    print (tempf.read())


import subprocess
import tempfile
for i in range(256):
    with tempfile.TemporaryFile() as temp_msa:
        proc = subprocess.Popen(['aws', 's3', 'ls',
                                 f's3://AF_data/true_structure_dataset/msa/msa_{i}/',
                                 '--recursive',
                                 '--human-readable',
                                 '--summarize'], stdout=temp_msa)
        proc.wait()
        temp_msa.seek(0)
        number_of_msa = int(temp_msa.readlines()[-2].split(b':')[-1])



    with tempfile.TemporaryFile() as temp_pdb:
        proc = subprocess.Popen(['aws', 's3', 'ls',
                                 f's3://AF_data/true_structure_dataset/msa/pdb_{i}/',
                                 '--recursive',
                                 '--human-readable',
                                 '--summarize'], stdout=temp_pdb)
        proc.wait()
        temp_pdb.seek(0)
        number_of_pdb = int(temp_pdb.readlines()[-2].split(b':')[-1])
        if number_of_msa != number_of_pdb:
            '''
            msa_list = temp_msa.readlines()[:-2]
            pdb_list = temp_pdb.readlines()[:-2]
            for pdb in pdb_list:
                if pdb.replace('.','/').split('/')[-2] not in msa_list:
            '''
            print(f'difference in pdb_{i}')
            print(number_of_pdb-number_of_msa)

        temp_pdb.close()
        temp_msa.close()
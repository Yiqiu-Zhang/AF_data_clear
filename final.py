import subprocess
from os import listdir, remove, system
import pickle
import numpy as np

ID_TO_HHBLITS_AA = {
    0: 'A',
    1: 'C',  # Also U.
    2: 'D',  # Also B.
    3: 'E',  # Also Z.
    4: 'F',
    5: 'G',
    6: 'H',
    7: 'I',
    8: 'K',
    9: 'L',
    10: 'M',
    11: 'N',
    12: 'P',
    13: 'Q',
    14: 'R',
    15: 'S',
    16: 'T',
    17: 'V',
    18: 'W',
    19: 'Y',
    20: 'X',  # Includes J and O.
    21: '-'
}
bucket_base = 's3://AF_data/'

def upload_item(msa_name):

    upload_command = 'aws s3 cp %s %s%s' % (msa_name, bucket_base, msa_name.replace('pkl','msa'))
    print(upload_command)
    system(upload_command)

def convert(name_list):
    for i in name_list:
        subprocess.call(['aws', 's3', 'cp',
                         f's3://AF_data_jinzhen/distillation_dataset/pkl/pkl_{i}.tar.gz',
                         f'distillation_dataset/pkl/pkl_{i}.tar.gz'])
        subprocess.call(['tar', '-xzvf', f'distillation_dataset/pkl/pkl_{i}.tar.gz',
                         '-C', f'distillation_dataset/pkl/'])
        subprocess.call(['rm',f'distillation_dataset/pkl/pkl_{i}.tar.gz'])
        pkl_files = listdir(f'distillation_dataset/pkl/pkl_{i}')

        while len(pkl_files) > 0:
            pkl_file = pkl_files.pop(0)
            print(pkl_file)
            with open(f'distillation_dataset/pkl/pkl_{i}/{pkl_file}', 'rb') as f:
                features = pickle.load(f)
                f.close()

            remove(f'distillation_dataset/pkl/pkl_{i}/{pkl_file}')

            msa_name = '.'.join([pkl_file.split('.')[0], 'aln'])
            with open(f'distillation_dataset/pkl/pkl_{i}/{msa_name}', 'w') as f:
                msa_array = np.vectorize(ID_TO_HHBLITS_AA.get)(features['msa'])
                f.write('\n'.join(''.join(seq) for seq in msa_array))
                f.close()
            upload_item(f'distillation_dataset/pkl/pkl_{i}/{msa_name}')
            remove(f'distillation_dataset/pkl/pkl_{i}/{msa_name}')

def uploadpdb(pdb_gz):

    for i in pdb_gz:
        subprocess.call(['aws', 's3', 'cp',
                         f's3://AF_data_jinzhen/true_structure_dataset/pdb/pdb_{i}.tar.gz',
                         f'true_structure_dataset/pdb/pdb_{i}.tar.gz'])
        subprocess.call(['tar', '-xzvf', f'true_structure_dataset/pdb/pdb_{i}.tar.gz',
                         '-C', f'true_structure_dataset/pdb/'])
        subprocess.call(['aws', 's3', 'cp',
                         f'true_structure_dataset/pdb/pdb_{i}',
                         f's3://AF_data/true_structure_dataset/pdb/pdb_{i}'])


if __name__ == '__main__':

    loadMSA_list = [9]
    loadpdb_list = []
    convert(loadMSA_list)
    uploadpdb(loadpdb_list)





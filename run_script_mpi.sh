#!/bin/bash
#SBATCH --job-name=baten_migration
#SBATCH --output=logs/result_%j.txt
#SBATCH --error=logs/error_%j.txt
#SBATCH --export=ALL,EXECUTABLE=./omp_exe
#SBATCH --partition=multiple_il
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1950mb
#SBATCH --time=24:00:00

export KMP_AFFINITY=compact,1,0

eval "$(/home/tu/tu_tu/tu_zxobe27/miniforge3/condabin/conda shell.bash hook)"
conda activate baten_migration

cd /pfs/work7/workspace/scratch/tu_zxobe27-baten_migration

mpirun -np 1 Rscript baten_migration_scrape_MPI.R
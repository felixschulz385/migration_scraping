#!/bin/bash
#SBATCH --job-name=baten_migration
#SBATCH --output=logs/result_%j.txt
#SBATCH --error=logs/error_%j.txt
#SBATCH --partition=multiple
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=40
#SBATCH --mem-per-cpu=1G
#SBATCH --time=720

eval "$(/home/tu/tu_tu/tu_zxobe27/miniforge3/condabin/conda shell.bash hook)"
conda activate baten_migration

cd /pfs/work7/workspace/scratch/tu_zxobe27-baten_migration

# Loop 4 times
for ((i=1; i<=4; i++)); do
    # Call the R script with the current index as an argument
    srun --exclusive Rscript baten_migration_scrape.R $i &
done

# Wait for all background jobs to finish
wait
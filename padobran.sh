#!/bin/bash

#PBS -N tvar
#PBS -l ncpus=8
#PBS -l mem=8GB
#PBS -J 1-2641
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image.sif tvar.R

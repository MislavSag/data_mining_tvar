
#!/bin/bash

#PBS -N TVAR
#PBS -l ncpus=8
#PBS -l mem=8GB
#PBS -J 3-2740
#PBS -o logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif tvar.R


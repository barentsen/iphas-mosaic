Mosaic all four-CCD exposures into single images.

= 1-imgtable.py =

Figures out the location of the images and confidence maps in the IPHAS raw data directory, and associated them to IPHAS field numbers and filter names.
Results are written to 'iphas-images.csv'

= 2-mosaic-mpi.pi =

MPI-enabled script which reads 'iphas-images.csv' and mosaics the four CCDs of each field into one.
This can be done on the cluster using 'qsub mosaic-all-runs.job'.

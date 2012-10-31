"""
Execute a list of commands using MPI
e.g. mpirun -np 4 python do-mosaic-mpi.py
"""
from mpi4py import MPI
import logging
import subprocess
import shlex
import os

def is_local():
    """Are we running locally or on the cluster?"""
    if os.uname()[1] == 'uhppc11.herts.ac.uk':
        return True
    return False


# Setup MPI and logging
comm = MPI.COMM_WORLD
logging.basicConfig(level=logging.DEBUG, 
    format="%(asctime)s/W"+str(comm.rank)+"/"+MPI.Get_processor_name()+"/%(levelname)s: %(message)s", 
    datefmt="%Y-%m-%d %H:%M:%S" )

# Where are the input images?
if is_local():
    in_dir = '/media/0133d764-0bfe-4007-a9cc-a7b1f61c4d1d/iphas'
else:
    in_dir = '/car-data/gb/iphas'
# Where to write the output?
if is_local():
    out_dir = '/tmp/iphas-mosaic'
else:
    out_dir = '/car-data/gb/iphas-mosaic'
# Mosaicking command
mosaic_cmd = '/home/gb/bin/casutools/bin/mosaic'
# fpack command (-D deletes original file, -Y suppresses warning)
fpack_cmd = '/home/gb/bin/cfitsio3310/bin/fpack -D -Y'

# Define the messages we'll be passing through MPI
GIVE_ME_WORK = 801  # Worker waiting for instructions
FINISHED = 850      # All work is done



def mpi_run():
    """Figure out whether we are master or worker"""
    if comm.rank==0:
        mpi_master()
    else:
        mpi_worker()
    return

def mpi_master():
    logging.info("Running on %d cores" % comm.size)

    images = open('iphas-images.csv', 'r')
    rows = images.readlines()
    # Ignore the first line of the CSV table (=header)
    for i, row in enumerate(rows[1:]):
        cols = row.strip().split(',')
        if cols[1] == "":
            continue

        # Wait for a worker to report for duty
        rank_done = comm.recv(source=MPI.ANY_SOURCE, tag=GIVE_ME_WORK)
        # Send the worker the details of the next image
        msg = {'field':cols[1], 'filter':cols[2], 'img':cols[3], 'conf':cols[4]}
        comm.send(msg, dest=rank_done)
        logging.info('Image %d/%d sent to worker %s' % (i, len(rows), rank_done))


    # Tell all workers we're finished
    for worker in range(1, comm.size):
        comm.send(FINISHED, dest=worker)
    
    return


def cmd_exec(cmd):
    """Execute a shell command"""
    logging.debug(cmd)
    p = subprocess.Popen(shlex.split(cmd), 
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = p.stdout.read().strip()
    stderr = p.stderr.read().strip()
    if stderr:
        logging.error("Error output detected! STDERR={%S} STDOUT={%s} CMD={%s}" % (stderr, stdout, cmd))
        raise Exception('Command failed: %s' % cmd)
    logging.debug( stdout )


def mpi_worker():
    while True:
        # Ask for work
        comm.send(comm.rank, dest=0, tag=GIVE_ME_WORK)
        msg = comm.recv(source=0)
        if msg == FINISHED:
            return

        # Perform work
        logging.debug("Message rcvd: \"%s\"" % msg)

        # Full paths of input/output images and confidence maps
        in_img = "%s/%s" % (in_dir, msg['img'])
        in_conf = "%s/%s" % (in_dir, msg['conf'])
        out_img = "%s/%s_%s_mosaic.fit" % (out_dir, msg['field'], msg['filter'])
        out_conf = "%s/%s_%s_conf.fit" % (out_dir, msg['field'], msg['filter'])
        

        commands = []
        # Casutools/mosaic command
        commands.append( "%s %s %s %s %s --skyflag=0 --verbose" \
                % (mosaic_cmd, in_img, in_conf, out_img, out_conf) )
        # Compression using fpack
        for filename in [out_img, out_conf]:
            if os.path.exists(filename+".fz"):
                commands.append( "rm %s.fz" % filename )
            commands.append( "%s %s" % (fpack_cmd, filename) )

        # Execute!
        try:
            for cmd in commands:
                cmd_exec(cmd)
        except:
            logging.error( "Aborted %s_%s" % (msg['field'], msg['filter']) )


""" MAIN """
mpi_run()
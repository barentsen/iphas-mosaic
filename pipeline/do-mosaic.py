import mosaic
import logging
import os
import sys

logging.basicConfig(level=logging.DEBUG,
    format="%(asctime)s/%(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S" )


""" CONFIGURATION """

# Machine-dependent settings
hostname = os.uname()[1]
if hostname == 'uhppc11.herts.ac.uk':
    SCRATCHDIR = '/home/gb/tmp/scratch3'
    IMAGEDIR = '/media/0133d764-0bfe-4007-a9cc-a7b1f61c4d1d/iphas'
elif hostname == 'stri-cluster.herts.ac.uk':
    SCRATCHDIR = '/tmp/scratch'
    IMAGEDIR = '/media/0133d764-0bfe-4007-a9cc-a7b1f61c4d1d/iphas'
elif len(sys.argv)==3:
    SCRATCHDIR = sys.argv[2]
    IMAGEDIR = sys.argv[1]
else:
    raise Exception('Script not configured for this machine.')

# Choose glon limits such that width = 192 deg = 64*3 deg
GLON1 = 26.5
GLON2 = 218.5

# Choose glat limits such that height = 12 deg = 4*3deg
GLAT1 = -6
GLAT2 = +6

# Tiling and resolution
RESOLUTION = 2*2 # arcsec/px
TILES_X = 64
TILES_Y = 4
TILES_OVERLAP = 0.05 # Fraction



def create_mosaic(tile, band):
    # Mosaic name
    name = "tile%03d-%s-normal" % (tile, band)

    # Create the header
    hdr_filename = '/tmp/%s.hdr' % name
    hdr = mosaic.FitsHeader(GLON1, GLON2,
                            GLAT1, GLAT2,
                            RESOLUTION,
                            TILES_X, TILES_Y, TILES_OVERLAP)
    hdr.save(hdr_filename, tile)
    # Montage performs better with an expanded header for the bgmodel
    hdr._tiles_overlap = 0.4
    hdr.save(hdr_filename+'.expanded', tile)

    # Go!
    m = mosaic.Mosaic(name, band, hdr_filename, IMAGEDIR, SCRATCHDIR)
    #m.mosaic()
    m.compute_overlaps()
    m.compute_background()


#m._clean_workdir()
#m.coadd()

create_mosaic(150, 'ha')

"""
Classes and functions implemented to mosaic the IPHAS survey.

"""

import logging
import os
import sys
import subprocess
import shlex
import pyfits
import numpy as np


class Mosaic(object):
    """
    Creates a single-filter mosaic using the Montage toolkit.

    :param name:
    Name of the mosaic.

    :param band:
    Filter (one of 'ha', 'r', 'i')
 
    :param header:
    Filename of the header.

    """

    def __init__(self, name, band, header, imagedir, scratchdir):
        # Preconditions
        assert(band in ['ha', 'r', 'i'])
        # Properties
        self._name = name
        self._band = band
        self._header = header
        # It is recommended to use an expanded header for reprojection+background steps!
        self._header_expanded = header+'.expanded'
        self._scratchdir = scratchdir
        
        # Configure locations of data and executable
        self._path = {}
        self._path['images'] = imagedir
        self._path['work'] = scratchdir+'/'+name
        self._path['output'] = scratchdir
        self._path['confmap'] = '/home/gb/dev/iphas-mosaic/confmap'
        self._path['montage'] = '/home/gb/bin/Montage_v3.3'
        self._path['fpack'] = '/home/gb/bin/cfitsio3310/bin'
        self._path['casutools'] = '/home/gb/bin/casutools/bin'
        self._path['iphas-meta'] = '/home/gb/dev/iphas-qc/data'
        self._path['imgtable'] = '/home/gb/dev/iphas-mosaic/imgtable'

        self._setup_log()

        self._imgtable_all = {'ha': '%s/iphas-images-best-ha.tbl' % self._path['imgtable'],
                              'r' : '%s/iphas-images-best-r.tbl' % self._path['imgtable'],
                              'i' : '%s/iphas-images-best-i.tbl' % self._path['imgtable']}

        # Filenames
        # Original images
        self._imgtable = '%s/img-%s.tbl' % (self._path['work'], self._name)
        # Projected images
        self._projtbl = '%s/proj-%s.tbl' % (self._path['work'], self._name)
        self._difftbl = '%s/diff-%s.tbl' % (self._path['work'], self._name)
        self._fittbl = '%s/fit-%s.tbl' % (self._path['work'], self._name)
        self._corrtbl = '%s/corr-%s.tbl' % (self._path['work'], self._name)

        self.use_mosaic = True
        self.conf_threshold = 90  # Confidence/weight map threshold

    def __del__(self):
        x = logging._handlers.copy()
        for i in x:
            self.log.removeHandler(i)
            i.flush()
            i.close()

    def _setup_log(self):
        # Setup logging
        self.log = logging.getLogger(self._name)
        self.log.setLevel(logging.DEBUG)

        fmt = logging.Formatter('%(asctime)s/'+self._name+'/%(levelname)s: %(message)s',
                                datefmt='%Y-%m-%d %H:%M:%S')

        # Log important messages to stdout
        # NOT NECESSARY WHEN DONE IN DO-MOSAIC
        #screen = logging.StreamHandler(stream=sys.stdout)
        #screen.setLevel(logging.INFO)
        #screen.setFormatter(fmt)   
        #self.log.addHandler(screen)

        # Log all debug messages to a file in the working directory
        logfile = logging.FileHandler('%s/log-%s.txt' % (
                                    self._path['output'], self._name))
        logfile.setLevel(logging.DEBUG)
        logfile.setFormatter(fmt)
        self.log.addHandler(logfile)

    def execute(self, cmd):
        """
        Executes a shell command and logs any errors.

        """
        self.log.debug(cmd)
        p = subprocess.Popen(shlex.split(cmd), 
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout = p.stdout.read().strip()
        stderr = p.stderr.read().strip()
        if stderr:
            self.log.error("STDERR={%s} STDOUT={%s} CMD={%s}" % (stderr, stdout, cmd))
            return False
        self.log.debug( stdout )
        return True

    def create_dir(self, path):
        """
        Create a directory, if it doesn't already exist.

        """
        if os.path.exists(path):
            self.log.warning('Dir already exists: %s' % path)
        else:
            self.execute('mkdir %s' % path)

    def setup_workdir(self):
        """
        Setup the working directory.

        """
        self.create_dir(self._scratchdir)
        self.create_dir(self._path['work'])
        self.create_dir(self._path['work']+'/orig') # Original images
        self.create_dir(self._path['work']+'/conf') # Original images
        self.create_dir(self._path['work']+'/proj') # Reprojected images
        self.create_dir(self._path['work']+'/diff') # Differences
        self.create_dir(self._path['work']+'/corr') # Background-corrected

        # Copy FITS header to working dir
        self.execute('cp %s %s' % (
            self._header, 
            self._path['work']) )
        self.execute('cp %s %s' % (
            self._header_expanded, 
            self._path['work']) )

    def _clean_workdir(self):
        self.execute('rm %s/diff/*' % self._path['work'])
        self.execute('rm %s/corr/*' % self._path['work'])
        #self._setup_workdir()

    def get_conf(self, image_filename):
        """
        Returns the filename of the confidence map for a given image.

        """
        """
        metadata = pyfits.getdata('/home/gb/dev/iphas-qc/data/iphas-observations.fits', 1)
        c = (metadata.field('image_'+self._band) == image_filename)
        if c.sum() > 0:
            return metadata.field('conf_'+self._band)[c][0]
        else:
            return ""
        """
        return ('%s/masterconf-%s-masked.fits' % (
                     self._path['confmap'], self._band) )

    def get_weightmap(self, image_filename):
        if self.use_mosaic:
            return '%s/conf/%s' % (self._path['work'], image_filename)
        else:
            return self.get_conf(image_filename)

    def select_images(self):
        # Identify images in the field
        self.execute( "%s/mCoverageCheck %s %s -header %s" % (
                    self._path['montage'], 
                    self._imgtable_all[self._band], 
                    self._imgtable, 
                    self._header) )

        tbl = open(self._imgtable, 'r')
        self._images = set() # Multi-HDU images appear multiple times
        for line in tbl.readlines()[3:]:
            self._images.add( line.strip().split(' ')[-1] )
        tbl.close()

    def copy_images(self):
        """
        Finds the correct set of images and copies them to the working dir.

        """
        # Copy
        commands = []
        for img in self._images:
            img_filename = img.split('/')[-1]


            if self.use_mosaic:
                commands.append( '%s/mosaic %s/%s %s %s/orig/%s %s/conf/%s --verbose --skyflag=0' % ( 
                                    self._path['casutools'],
                                    self._path['images'], 
                                    img,
                                    self.get_conf(img_filename),
                                    self._path['work'],
                                    img_filename,
                                    self._path['work'],
                                    img_filename
                                     ) )
            else:
                commands.append( 'cp %s/%s %s/orig/%s.fz' % (
                                    self._path['images'], 
                                    img, 
                                    self._path['work'], 
                                    img_filename) )

                # IPHAS images are compressed with fpack by default;
                # the files need to be decompressed first
                commands.append( '%s/funpack -D %s/orig/%s.fz' % (
                                    self._path['fpack'], 
                                    self._path['work'], 
                                    img_filename) )



        for i, cmd in enumerate(commands):
            self.log.info('Copying files: command %d out of %d' % (i+1, len(commands)))
            self.execute(cmd)

    def compute_projections(self):
        """
        Re-project the images.

        """
        assert( os.path.exists( self._path['work'] ) )
        assert( self._images != None )
        assert( len(self._images) > 0 )

        commands = []

        # Which HDU's need to be reprojected?
        if self.use_mosaic:
            hdulist = [0]
        else:
            hdulist = [1,2,3,4]

        # Reproject all images in this loop
        for i, img in enumerate(self._images):
            self.log.info('Reprojecting image %d out of %d' % (
                            i+1, len(self._images)))

            # Filename without path
            img_filename = img.split('/')[-1]
            # Full filename with new path
            img_orig = '%s/orig/%s' % (
                            self._path['work'], 
                            img_filename)

            # Montage requires the equinox keyword to be '2000.0'
            # but CASUtools sets the value 'J2000.0'
            if self.use_mosaic:
                myfits = pyfits.open(img_orig)
                myfits[0].header.update('EQUINOX', '2000.0')
                myfits.writeto(img_orig, clobber=True)

            for hdu in hdulist:
                cmd = '%s/mProject -w %s -t %s -h %d %s/orig/%s %s/proj/hdu%d_%s %s' % (
                                self._path['montage'], 
                                self.get_weightmap(img_filename), 
                                self.conf_threshold,
                                hdu, 
                                self._path['work'], 
                                img_filename, 
                                self._path['work'], 
                                hdu, 
                                img_filename, 
                                self._header_expanded )
                self.execute(cmd)

        # Create a new image table for the re-projected images
        cmd = '%s/mImgtbl -c %s/proj %s' % (
                self._path['montage'], 
                self._path['work'], 
                self._projtbl)
        self.execute(cmd)

        # Co-add without background correction
        output_uncorrected = '%s/%s-uncorrected.fits' % (self._path['work'], self._name)
        cmd = '%s/mAdd -d 1 -a mean -e -p %s/proj %s %s %s' % (
                    self._path['montage'], 
                    self._path['work'], 
                    self._projtbl, 
                    self._header_expanded, 
                    output_uncorrected)
        self.execute(cmd)

        # Produce a quicklook jpg
        cmd = '%s/mJPEG -gray %s 20 200 log -out %s' % (
                    self._path['montage'], 
                    output_uncorrected,
                    output_uncorrected + '.jpg')
        self.execute(cmd)


    def compute_overlaps(self):
        """
        Co-add the images after re-projection

        """
        assert( os.path.exists( self._path['work'] ) )

        # Where do the images overlap?
        cmd = '%s/mOverlaps %s %s' % (
                            self._path['montage'], 
                            self._projtbl, 
                            self._difftbl)
        self.execute(cmd)

        # Compute the difference between all overlapping pairs
        cmd = '%s/mDiffExec -p %s/proj %s %s %s/diff' % (
                    self._path['montage'], 
                    self._path['work'], 
                    self._difftbl, 
                    self._header_expanded,
                    self._path['work'])
        self.execute(cmd)

        # Fit a plane through the mosaic
        cmd = '%s/mFitExec %s %s %s/diff' % (
                    self._path['montage'], 
                    self._difftbl, 
                    self._fittbl, 
                    self._path['work'])
        self.execute(cmd)

    def compute_background(self):
        # Copy all projected images to avoid non-overlapping ones to be missing
        """
        self.execute('rm -r %s/corr' % self._path['work'])
        cmd = 'cp -a %s/proj %s/corr' % (self._path['work'], self._path['work'])
        self.execute(cmd)
        """
        # Determine the set of corrections to apply
        cmd = '%s/mBgModel -l -i 20000 %s %s %s' % (
                self._path['montage'], 
                self._projtbl, 
                self._fittbl, 
                self._corrtbl)
        self.execute(cmd)

        # Apply the corrections
        cmd = '%s/mBgExec -p %s/proj %s %s %s/corr' % (
                self._path['montage'], 
                self._path['work'], 
                self._projtbl, 
                self._corrtbl, 
                self._path['work'])
        self.execute(cmd)

        # Create a new image table for the corrected images
        self._corrimgtbl = '%s/corrimg-%s.tbl' % (self._path['work'], self._name)
        cmd = '%s/mImgtbl -c %s/corr %s' % (
                self._path['montage'], 
                self._path['work'], 
                self._corrimgtbl)
        self.execute(cmd)

        # Co-add the corrected images
        output_corrected_local = '%s/%s.fits' % (
                            self._path['work'], 
                            self._name)
        cmd = '%s/mAdd -a mean -e -p %s/corr %s %s %s' % (
                self._path['montage'], 
                self._path['work'], 
                self._corrimgtbl, 
                self._header, 
                output_corrected_local)
        self.execute(cmd)

        # Move the result to the requested output directory
        output_corrected = '%s/%s.fits' % (
                            self._path['output'], 
                            self._name)
        cmd = 'cp %s %s' % (output_corrected_local, output_corrected)
        self.execute(cmd)

        # Produce a quicklook jpg
        cmd = '%s/mJPEG -gray %s 20 200 log -out %s' % (
                    self._path['montage'], 
                    output_corrected,
                    output_corrected + '.jpg')
        self.execute(cmd)

    def mosaic(self):
        """
        Create the mosaic
        """
        self.setup_workdir()
        self.select_images()
        #self.copy_images()
        self.compute_projections()
        self.compute_overlaps()
        self.compute_background()
        self.log.info('All is said and done.')


class FitsHeader(object):
    """
    Creates tiled FITS WCS headers for large-scale mosaics.

    :param x1: (degrees)
    :param x2: (degrees)
    Limits in horizontal direction.

    :param y1: (degrees)
    :param y2: (degrees)
    Limits in vertical direction.

    :param resolution: (arcsec/px)
    Pixel resolution.

    :param tiles_x:
    Number of tiles in the longitude direction.

    :param tiles_y:
    Number of tiles in the latitude direction.

    :param tiles_overlap:
    Fraction of overlap between tiles.
    """

    def __init__(self, x1, x2, y1, y2, resolution, 
            tiles_x, tiles_y, tiles_overlap=0.05,
            ctype1='GLON-CAR', ctype2='GLAT-CAR'):
        self._x1, self._x2 = x1, x2
        self._y1, self._y2 = y1, y2
        self._resolution = resolution
        self._tiles_x = tiles_x
        self._tiles_y = tiles_y
        self._tiles_overlap = tiles_overlap
        self._ctype1 = ctype1
        self._ctype2 = ctype2

    def parse(self, tile=0):
        """
        Parses the FITS header for a given tile number.

        :param tile:
        Tile number (starting at zero.)

        """
        # Compute resolution in degrees/px
        self._cdelt1 = -self._resolution/3600.
        self._cdelt2 = self._resolution/3600.

        # Compute the horizontal size of each tile
        self._xsize = (self._x2 - self._x1) / float(self._tiles_x)
        naxis1 = self._xsize / -self._cdelt1
        self._naxis1 = (1.0 + 2*self._tiles_overlap) * naxis1
        # Compute the vertical size of each tile
        self._ysize = (self._y2 - self._y1) / float(self._tiles_y)
        naxis2 = self._ysize / self._cdelt2
        self._naxis2 = (1.0 + 2*self._tiles_overlap) * naxis2

        # Log our findings for debugging
        logging.debug( ('Tile parameters: NAXIS1=%s (%s deg) NAXIS2=%s (%s deg)'
                       +' CDELT1=%s CDELT2=%s') % (
                        self._naxis1, self._xsize, 
                        self._naxis2, self._ysize,
                        self._cdelt1, self._cdelt2))

        # x and y number of the tile (starting at zero)
        x = int(np.floor(tile / self._tiles_y))
        y = (tile % self._tiles_y)

        # Sky coordinates at the center of the tile
        crval1 = self._x1 + (x+0.5) * self._xsize
        crval2 = self._y1 + (y+0.5) * self._ysize
        # Pixel coordinates at the center of the tile
        crpix1 = self._naxis1 / 2
        crpix2 = self._naxis2 / 2
        # Log our findings for debugging
        logging.debug('x=%s y=%s CRVAL1=%s CRVAL2=%s CRPIX1=%s CRPIX2=%s' % (
                        x, y, crval1, crval2, crpix1, crpix2))

        # Parse the header
        hdr = ("SIMPLE  = T\n"
                +"BITPIX  = -32\n"
                +"NAXIS   = 2\n"
                +"NAXIS1  = %d\n"
                +"NAXIS2  = %d\n"
                +"CTYPE1  = '%s'\n"
                +"CTYPE2  = '%s'\n"
                +"EQUINOX = 2000\n"
                +"CRVAL1  =  %.7f\n"
                +"CRVAL2  =   %.7f\n"
                +"CRPIX1  =   %.7f\n"
                +"CRPIX2  =   %.7f\n"
                +"CDELT1  =    %.14f\n"
                +"CDELT2  =    %.14f\n"
                +"PC1_1 = 1\n"
                +"PC1_2 = 0\n"
                +"PC2_1 = 0\n"
                +"PC2_2 = 1\n"
                +"END\n") % (
                self._naxis1, self._naxis2,
                self._ctype1, self._ctype2,
                crval1, crval2, crpix1, crpix2, 
                self._cdelt1, self._cdelt2)

        return hdr

    def save(self, filename, tile=0):
        """
        Writes the header to a file.

        :param filename:
        Filename to write the header to.

        :param tile:
        Tile number (starting at zero.)
        """
        header = self.parse(tile)
        output = open(filename, 'w')
        output.write(header)
        output.close()


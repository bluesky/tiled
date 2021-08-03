import builtins
import numpy as np

from ..structures.array import (
    ArrayMacroStructure,
    MachineDataType,
)

class TiffSequenceReader:
    
    structure_family = "array"
    
    def __init__(self, seq):
        self._seq = seq
        self._metadata = {}
        
    @property
    def metadata(self):
        return self._metadata

    def read(self, slice=None):
        """Return a numpy array

        Receives a sequence of values to select from a collection of tiff files that were saved in a folder
        The input order is defined as files --> X slice --> Y slice
        read() can receive one value or one slice to select all the data from one file or a sequence of files;
        or it can receive a tuple of up to three values (int or slice) to select a more specific sequence of pixels
        of a group of images
        """    
        
        print("Inside Reader:", slice)
        if slice is None:            
            return self._seq.asarray()
        if isinstance(slice, int):
            # e.g. read(slice=0)
            return self._seq.asarray(file=slice)
        # e.g. read(slice=(...))
        if isinstance(slice, tuple):
            image_axis, *the_rest = slice
            # Could be int or slice
            # (0, slice(...)) or (0,....) are converted to a list
            if isinstance(image_axis, int):
                # e.g. read(slice=(0, ....))
                arr = self._seq.asarray(file=image_axis)
            if isinstance(image_axis, builtins.slice):
                if image_axis.start is None:
                    slice_start = 0
                else:
                    slice_start = image_axis.start
                if image_axis.step is None:
                    slice_step = 1
                else:
                    slice_step = image_axis.step
                arr = np.stack(
                    [
                        self._seq.asarray(file=i)
                        for i in range(slice_start, image_axis.stop, slice_step)
                    ]
                )
            arr = arr[tuple(the_rest)]
            return arr
        if isinstance(slice, builtins.slice):
            # Check for start and step which can be optional
            if slice.start is None:
                slice_start = 0
            else:
                slice_start = slice.start
            if slice.step is None:
                slice_step = 1
            else:
                slice_step = slice.step
            arr = np.stack(
                [
                    self._seq.asarray(file=i)
                    for i in range(slice_start, slice.stop, slice_step)
                ]
            )
            return arr

    def read_block(self, block, slice=None):
        print("Block:", block)
        if block[1:] != (0, 0):
            raise IndexError(block)
        arr = self.read(builtins.slice(block[0],block[0]+1))
        if slice is not None:
            arr = arr[slice]
        return arr
        
    def microstructure(self):
        # Assume all files have the same data type
        return MachineDataType.from_numpy_dtype(self.read(slice=0).dtype)
        
    def macrostructure(self):
        shape = (len(self._seq), *self.read(slice=0).shape)
        #print("array shape", shape)
        return ArrayMacroStructure(
            shape=shape,
            # one chunks per underlying TIFF file
            chunks = (
                (1,) * shape[0],
                (shape[1],),
                (shape[2],),
        ))
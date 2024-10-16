import builtins
from pathlib import Path
from typing import Any, List, Optional, Tuple

import numpy as np
from numpy._typing import NDArray
from PIL import Image

from ..structures.array import ArrayStructure, BuiltinDtype
from ..structures.core import Spec, StructureFamily
from ..utils import path_from_uri
from .protocols import AccessPolicy
from .resource_cache import with_resource_cache
from .type_alliases import JSON, NDSlice


class JPEGAdapter:
    """
    Read a JPEG file.

    Examples
    --------

    >>> JPEGAdapter("path/to/file.jpeg")
    """

    structure_family = StructureFamily.array

    def __init__(
        self,
        data_uri: str,
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        data_uri :
        structure :
        metadata :
        specs :
        access_policy :
        """
        if not isinstance(data_uri, str):
            raise Exception
        filepath = path_from_uri(data_uri)
        cache_key = (Image.open, filepath)
        self._file = with_resource_cache(cache_key, Image.open, filepath)
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            arr = np.asarray(self._file)
            structure = ArrayStructure(
                shape=arr.shape,
                chunks=tuple((dim,) for dim in arr.shape),
                data_type=BuiltinDtype.from_numpy_dtype(arr.dtype),
            )
        self._structure = structure

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        return {}

    def read(self, slice: Optional[NDSlice] = None) -> NDArray[Any]:
        """

        Parameters
        ----------
        slice :

        Returns
        -------

        """
        arr = np.asarray(self._file)
        if slice is not None:
            arr = arr[slice]
        return arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[slice] = None
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        if sum(block) != 0:
            raise IndexError(block)

        arr = np.asarray(self._file)
        if slice is not None:
            arr = arr[slice]
        return arr

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure


class JPEGSequenceAdapter:
    """ """

    structure_family = "array"

    @classmethod
    def from_uris(
        cls,
        data_uris: List[str],
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> "JPEGSequenceAdapter":
        """

        Parameters
        ----------
        data_uris :
        structure :
        metadata :
        specs :
        access_policy :

        Returns
        -------

        """
        seq = [path_from_uri(data_uri) for data_uri in data_uris]
        return cls(
            seq,
            structure=structure,
            specs=specs,
            metadata=metadata,
            access_policy=access_policy,
        )

    def __init__(
        self,
        seq: List[Path],
        *,
        structure: Optional[ArrayStructure] = None,
        metadata: Optional[JSON] = None,
        specs: Optional[List[Spec]] = None,
        access_policy: Optional[AccessPolicy] = None,
    ) -> None:
        """

        Parameters
        ----------
        seq :
        structure :
        metadata :
        specs :
        access_policy :
        """
        self._seq = seq
        # TODO Check shape, chunks against reality.
        self.specs = specs or []
        self._provided_metadata = metadata or {}
        self.access_policy = access_policy
        if structure is None:
            shape = (len(self._seq), *self.read(slice=0).shape)
            structure = ArrayStructure(
                shape=shape,
                # one chunks per underlying TIFF file
                chunks=((1,) * shape[0], *[(i,) for i in shape[1:]]),
                # Assume all files have the same data type
                data_type=BuiltinDtype.from_numpy_dtype(self.read(slice=0).dtype),
            )
        self._structure = structure

    def metadata(self) -> JSON:
        """

        Returns
        -------

        """
        # TODO How to deal with the many headers?
        return self._provided_metadata

    def read(self, slice: Optional[NDSlice] = ...) -> NDArray[Any]:
        """Return a numpy array

        Receives a sequence of values to select from a collection of jpeg files
        that were saved in a folder The input order is defined as: files -->
        vertical slice --> horizontal slice --> color slice --> ... read() can
        receive one value or one slice to select all the data from one file or
        a sequence of files; or it can receive a tuple (int or slice) to select
        a more specific sequence of pixels of a group of images.

        Parameters
        ----------
        slice :

        Returns
        -------
                Return a numpy array

        """

        if slice is Ellipsis:
            return np.asarray([np.asarray(Image.open(file)) for file in self._seq])
        if isinstance(slice, int):
            # e.g. read(slice=0) -- return an entire image
            return np.asarray(Image.open(self._seq[slice]))
        if isinstance(slice, builtins.slice):
            # e.g. read(slice=(...)) -- return a slice along the image axis
            return np.asarray(
                [np.asarray(Image.open(file)) for file in self._seq[slice]]
            )
        if isinstance(slice, tuple):
            if len(slice) == 0:
                return np.asarray([np.asarray(Image.open(file)) for file in self._seq])
            if len(slice) == 1:
                return self.read(slice=slice[0])
            image_axis, *the_rest = slice
            # Could be int or slice (0, slice(...)) or (0,....); the_rest is converted to a list
            if isinstance(image_axis, int):
                # e.g. read(slice=(0, ....))
                arr = np.asarray(Image.open(self._seq[image_axis]))
            elif image_axis is Ellipsis:
                # Return all images
                arr = np.asarray([np.asarray(file) for file in self._seq])
                the_rest.insert(0, Ellipsis)  # Include any leading dimensions
            elif isinstance(image_axis, builtins.slice):
                arr = self.read(slice=image_axis)
            arr = np.atleast_1d(arr[tuple(the_rest)])
            return arr

    def read_block(
        self, block: Tuple[int, ...], slice: Optional[NDSlice] = ...
    ) -> NDArray[Any]:
        """

        Parameters
        ----------
        block :
        slice :

        Returns
        -------

        """
        if any(block[1:]):
            raise IndexError(block)
        arr = self.read(builtins.slice(block[0], block[0] + 1))
        return arr[slice]

    def structure(self) -> ArrayStructure:
        """

        Returns
        -------

        """
        return self._structure

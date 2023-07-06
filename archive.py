import logging
import hashlib
import pathlib
import requests
import json
import shutil
import time
import re

class Archive:
    """A class for communcation with an archive.

    Supports two different archives: a local file, and a server running
    the code at https://github.com/c3-time-domain/nersc-upload-connector.

    It usually only makes sense to have one of archive_url or
    local_read_dir not None, though the code will merrily write to
    both locations if both are given.  (For downloading, it will prefer
    local_read_dir over url if both are specified.)  The latter is intended as
    a performance boost when the archive server is writing to a disk
    that's locally accessible on the machine where this code is running.
    (In that case, specifying both local_read_dir and url will cause the file
    to first be copied to the directory, and then sent to the archive
    server, which is redundant.)

    Call upload() to push something to the archive, get_info() to get
    information about something on the archive, and download() to pull
    something from the archive.

    """

    def __init__( self,
                  archive_url=None,
                  path_base=None,
                  token=None,
                  verify_cert=False,
                  local_read_dir=None,
                  local_write_dir=None,
                  logger=logging.getLogger("main") ):
        """Construct an Archive object

        archive_url - URL of the server running the upload-connector code
        path_base - the base path, or "collection", that we're archiving to
        token - the token for the server that corresponds to path_base
        verify_cert - if False, don't bother verifying the server's SSL certificate (i.e. live dangerously)
        local_read_dir - a local directory that serves as the archive; path_base must be a subdirectory there.
            This is the directory used for reading.  It is usually the same as local_write_dir, but they
            might be different in case the same filesystem is mounted in two different ways such that
            it's more efficient to read from one way of mounting it than another.
        local_write_dir - See local_read_dir; if this is None, defaults to local_read_dir
        logger - a logging.Logger object (defaults to getting the "main" logger)

        It usually doesn't make sense to have both archive_url and
        local_read_dir not None, although the code will accept it.  On
        get_info or download, it will use the local_read_dir first.  On
        upload, it will do *both*.  The usual use case for local_read_dir is
        when this code is running where the filesystem that the archive
        writes to is locally available.  In that case, if local_read_dir and
        archive_url are both non-None, it will first copy the file, then
        send it through the upoad server, which is redundant.

        """
        if ( local_read_dir is None) and ( archive_url is None ):
            raise ValueError( "Archive: one of local_read_dir or archive_url must be non-None" )
        if local_write_dir is None:
            local_write_dir = local_read_dir
        
        self.logger = logger
        self.url = archive_url
        if ( self.url is not None ) and ( self.url[-1] == '/' ):
            self.url = self.url[:-1]
        self.path_base = pathlib.Path( path_base )
        self.token = token
        self.local_read_dir = None if local_read_dir is None else pathlib.Path( local_read_dir )
        self.local_write_dir = None if local_write_dir is None else pathlib.Path( local_write_dir )
        self.verify_cert = verify_cert
        
    # ======================================================================

    def _retry_request( self, endpoint, data={}, files=None, isjson=True, downloadfile=None,
                        retries=5, sleeptime=2, expectederror=None ):
        """Send a request to the archive server with retries.

        endpoint - the part of the URL after self.url
        data - post data; a dict that will be json encoded by passing it to the json= argument of requests.post
        files - upload file info (passed to python requests with files=), or None (default)
        isjson - true if we expect a json response, false otherwise (default True)
        downloadfile - path of binary file to download, or None if none is expected (default None)
        retries - number of times to retry if there's a communications failure (default 5)
        sleeptime - time to sleep (in seconds) after a failure before retrying (default 2)
        expectederror - see below

        If succesful, will return the data structure loaded from the
        returned json (if isjson is True) or True (if downloadfile is
        not None).

        If the first try returns an error response (so, a valid return
        from the server, but with a json encoded dictionary that has an
        "error" field), and if expectederror is not None, and the
        beginning of the value of the "error" field of the returned
        dictionary matches expectederror, returns None.

        Otherwise, will raise an exception.

        """
        
        url = f"{self.url}/{endpoint}"
        if ( not isjson ) and ( downloadfile is None ):
            raise RuntimeError( "isjson is false, and downloadfile is None... I don't know what to do with {url}" )
            
        countdown = retries
        while countdown >= 0:
            res = None
            try:
                res = requests.post( f"{url}", data=data, files=files, verify=self.verify_cert )
            except Exception as ex:
                self.logger.warning( f"Got exception {ex} trying to contact {url} with data {data}" )
            else:
                if res.status_code != 200:
                    self.logger.warning( f"Got status_code={res.status_code} from {url} with data {data}" )
                if isjson:
                    if res.headers['content-type'] != 'application/json':
                        self.logger.warning( f"Server returned {res.headers['content-type']}, expected json" )
                    else:
                        resval = json.loads( res.text )
                        if "error" in resval:
                            if ( ( expectederror is not None) and
                                 ( resval['error'][:len(expectederror)] == expectederror ) ):
                                return None
                            if resval['error'][0:13] == 'Invalid token':
                                self.logger.error( f"Invalid token for {url}" )
                                raise RuntimeError( f"Invalid token for archive server" )
                            else:
                                tb = resval['traceback'] if 'traceback' in resval else '(No traceback)'
                                self.logger.warning( f"Got error response {resval['error']} from {url} "
                                                     f"with data {data}\n{tb}" )
                        else:
                            return resval
                elif downloadfile is not None:
                    if res.headers['content-type'] != 'application/octet-stream':
                        self.logger.warning( f"Server returned {res.headers['content-type']}, "
                                             f"expected an octet stream" )
                    else:
                        with open( downloadfile, "wb" ) as ofp:
                            ofp.write( res.content )
                        return True
                else:
                    raise RuntimeError( "This should never happen." )
            finally:
                try:
                    res.close()
                except Exception:
                    pass
                    
            # If we haven't returned, then it's an error of some sort, and we should keep counting down
            countdown -= 1
            if countdown >= 0:
                self.logger.warning( f"Failed to post to {url} with data {data}; "
                                     f"will sleep {sleeptime}s and retry." )

        raise RuntimeError( f"Repeated failures trying to post to {url} with data {data}" )
                
    # ======================================================================
        
    def upload( self, localpath, remotedir=None, remotename=None, overwrite=True ):
        """Upload/copy a file to the archive.
        
        localpath - path (string or pathlib.Path object) of the local file
        remotedir - The subdirectory (underneath self.path_base) where the file should live on the archive
        remotename - The name of the file on the archive (defaults to the filename part of localpath)
        overwrite - Boolean, should we overwrite the archive file if it already exists?

        Returns the md5sum of the file in the archive if succesful.  Raises an exception if not.

        """

        localpath = pathlib.Path( localpath )
        remotename = remotename if remotename is not None else localpath.name
        if remotedir is not None:
            serverpath = self.path_base / remotedir / remotename
        else:
            serverpath = self.path_base / remotename

        if not localpath.is_file():
            raise FileNotFoundError( f"Can't find file {localpath} to upload to archive!" )
        md5 = hashlib.md5()
        with open( localpath, "rb" ) as ifp:
            md5.update( ifp.read() )
        localmd5 = md5.hexdigest()
        md5sum = None

        if self.local_write_dir is not None:
            destpath = self.local_write_dir / serverpath
            if destpath.exists():
                if not destpath.is_file():
                    raise RuntimeError( f"Failed to copy to archive; {destpath} exists and isn't a normal file!" )
                if overwrite:
                    destpath.unlink()
                else:
                    raise RuntimeError( f"Failed to copy, {destpath} already exists on archive "
                                        f"and overwrite was False" )
            if destpath.parent.exists() and not destpath.parent.is_dir():
                raise RuntimeError( f"Failed to copy to archive; destination directory {destpath.parent} "
                                    f"exists, but is not a directory!" )
            destpath.parent.mkdir( parents=True, exist_ok=True )
            shutil.copy2( localpath, destpath )
            md5 = hashlib.md5()
            with open( destpath, "rb" ) as ifp:
                md5.update( ifp.read() )
            md5sum = md5.hexdigest()
            if md5sum != localmd5:
                destpath.unlink()
                raise RuntimeError( f"Tried to copy {localpath} to {destpath}, but destination file had "
                                    f"md5sum {md5sum}, which doesn't match source {localmd5}" )

        if self.url is not None:
            data = { "overwrite": int(overwrite),
                     "path": str(serverpath),
                     "dirmode": 0o755,
                     "mode": 0o644,
                     "token": self.token,
                     "md5sum": md5sum }
            ifp = open( localpath, "rb" )
            filedata = { "fileinfo": ifp }
            try:
                resval = self._retry_request( f"upload", data=data, files=filedata,
                                              expectederror='File already exists' )
                if ( resval is None ) and ( not overwrite ):
                    raise RuntimeError( f"Failed to upload, {serverpath} already exists on archive "
                                        f"and overwrite was False" )
            finally:
                ifp.close()
            md5sum = resval['md5sum']
            if md5sum != localmd5:
                raise RuntimeError( f"Failed to upload {localpath} to server {serverpath}; "
                                    f"server returned md5sum {md5sum}, which doesn't match "
                                    f"local {localmd5}.  This exception never happen; the server "
                                    f"should have already raised an exception from the mismatch." )

        if md5sum is None:
            raise RuntimeError( "This should never happen; md5sum is None at the end of Archive.upload(). "
                                "An exception should already have been raised." )
        return md5sum
    
    # ======================================================================

    def get_info( self, serverpath ):
        """Get information about a file on the server

        serverpath - path on server relative to self.path_base (string or pathlib.Path object)

        Returns None if the file isn't found on the archive.

        Otherwise, returns a dictionary with:
           serverpath : absolute path of file on archive (string)
           size : size of file on archive
           md5sum : md5sum of file on archive

        """

        if self.local_read_dir is not None:
            archivepath = self.local_read_dir / self.path_base / serverpath
            if not archivepath.exists():
                return None
            if not archivepath.is_file():
                raise RuntimeError( f"Archive file {architepath} exists but is not a regular file!" )
            md5 = hashlib.md5()
            with open( archivepath, "rb" ) as ifp:
                md5.update( ifp.read() )
            stat = archivepath.stat()
            return { "serverpath": str(archivepath),
                     "size": stat.st_size,
                     "md5sum": md5.hexdigest() }

        else:
            data = { "path": str( self.path_base / serverpath ), "token": self.token }
            res = self._retry_request( "getfileinfo", data=data, expectederror='No such file' )
            return res
            
    # ======================================================================

    def delete( self, serverpath, okifmissing=True ):
        """Delete a file in the archive

        serverpath - path of file to delete relative to self.path_base
        okifmissing - if False, then raise an exception if the file isn't there
        
        returns True if it thinks it worked, otherwise raises an exception
        """

        if self.local_write_dir is not None:
            archivepath = self.local_write_dir / self.path_base / serverpath
            if archivepath.exists():
                if not archivepath.is_file():
                    raise RuntimeError( f"Archive file {archivepath} exists but is not a regular file!" )
                archivepath.unlink()
            elif not okifmissing:
                raise FileNotFoundError( f"Can't delete archive file {archivepath}, it doesn't exist." )

        if self.url is not None:
            archivepath = self.path_base / serverpath
            data = { "path": str(archivepath),
                     "token": self.token,
                     "overwrite": 1,
                     "okifmissing": okifmissing
                    }
            self._retry_request( "delete", data=data )

        return True
    
    # ======================================================================

    def download( self, serverpath, localpath, verifymd5=False, clobbermismatch=True ):
        """Copy a file from the archive to local storage.

        serverpath - string or pathlib.Path, path relative to path_base on the server
        localpath - string or pathlib.Path, absolute path to where file should be saved locally
        verifymd5 - if False, and the file already exists locally, don't do anything.  If 
           True, and the file already exists locally, will ask the archive for the corresponding
           file's md5sum to compare to the local file's md5sum
        clobbermismatch - if verifymd5 is True and the archive's md5sum
          doesn't match the local file's mismatch, then if this is True,
          overwrite the local file with the one from the server;
          otherwise, raise an exception.

        Returns True if succesful, otherwise raises an exception.
        
        """

        localpath = pathlib.Path( localpath )
        localmd5 = None
        if localpath.exists():
            if not localpath.is_file():
                raise RuntimeError( f"{localpath} exists but isn't a regular file!" )
            elif not verifymd5:
                return True
            md5 = hashlib.md5()
            with open( localpath, "rb" ) as ifp:
                md5.update( ifp.read() )
            localmd5 = md5.hexdigest()
        
        serverpath = self.path_base / serverpath

        finished = False
        
        if self.local_read_dir is not None:
            srcpath = pathlib.Path( self.local_read_dir ) / serverpath
            if not srcpath.exists():
                raise FileNotFoundError( f"Could not find archive file {serverpath}" )
            md5 = hashlib.md5()
            with open( srcpath, "rb" ) as ifp:
                md5.update( ifp.read() )
            md5sum = md5.hexdigest()

            if ( localmd5 is not None ) and ( localmd5 != md5sum ):
                if clobbermismatch:
                    localpath.unlink()
                else:
                    raise RuntimeError( "Local file {localpath} exists but md5sum doesn't match "
                                        "{srcpath} on archive; local={localmd5}, archive={md5sum}" )

            # If we get this far and localfile exists, then we know we don't want to overwrite it
            if not localpath.exists():
                shutil.copy2( self.local_read_dir / serverpath, localpath )
                md5 = hashlib.md5()
                with open( localpath, "rb" ) as ifp:
                    md5.update( ifp.read() )
                localmd5 = md5.hexdigest()
                if localmd5 != md5sum:
                    localpath.unlink()
                    raise RuntimeError( "Error copying from archive {serverpath} to {localpath}; "
                                        "md5sum mismatch: archive {md5sum}, local {md5.hexdigest()}" )
            finished = True

        if ( not finished ) and ( self.url is None ):
            raise RuntimeError( "Haven't been able to copy file from local archive, and there's no url!" )

        if not finished:
            data = { "path": str(serverpath), "token": self.token }
            resval = self._retry_request( f"getfileinfo", data=data )
            md5sum = resval['md5sum']
            if localmd5 is not None:
                if localmd5 != md5sum:
                    if clobbermismatch:
                        localpath.unlink()
                    else:
                        raise RuntimeError( f"Local file {localpath} exists but md5sum doesn't match "
                                            f"{serverpath} on archive; local={localmd5}, server={md5sum}" )

            # If we get this far and localpath exists, we know we're done
            if not localpath.exists():
                self._retry_request( f"download", data=data, isjson=False, downloadfile=localpath )
                md5 = hashlib.md5()
                with open( localpath, "rb" ) as ifp:
                    md5.update( ifp.read() )
                localmd5 = md5.hexdigest()
                if md5sum != localmd5:
                    localpath.unlink()
                    raise RuntimeError( f"Failed to download archive file {serverpath} to {localpath}; "
                                        f"local md5sum {md5.hexdigest()} did not match server's {md5sum}" )

        return True

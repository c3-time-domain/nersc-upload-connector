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
    archive_copy_dir not None, though the code will merrily write to
    both locations if both are given.  (For downloading, it will prefer
    copy_dir over url if both are specified.)  The latter is intended as
    a performance boost when the archive server is writing to a disk
    that's locally accessible on the machine where this code is running.
    (In that case, specifying both copy_dir and url will cause the file
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
                  copy_dir=None,
                  logger=logging.getLogger("main") ):
        """Construct an Archive object

        archive_url - URL of the server running the upload-connector code
        path_base - the base path, or "collection", that we're archiving to
        token - the token for the server that corresponds to path_base
        verify_cert - if False, don't bother verifying the server's SSL certificate (i.e. live dangerously)
        copy_dir - a local directory that serves as the archive; path_base must be a subdirectory there
        logger - a logging.Logger object (defaults to getting the "main" logger)

        It usually doesn't make sense to have both archive_url and
        copy_dir not None, although the code will accept it.  On
        get_info or download, it will use the copy_dir first.  On
        upload, it will do *both*.  The usual use case for copy_dir is
        when this code is running where the filesystem that the archive
        writes to is locally available.  In that case, if copy_dir and
        archive_url are both non-None, it will first copy the file, then
        send it through the upoad server, which is redundant.

        """
        if ( copy_dir is None) and ( archive_url is None ):
            raise ValueError( "Archive: one of copy_dir or archive_url must be non-None" )
        
        self.logger = logger
        self.url = archive_url
        if ( self.url is not None ) and ( self.url[-1] == '/' ):
            self.url = self.url[:-1]
        self.path_base = pathlib.Path( path_base )
        self.token = token
        self.copy_dir = None if copy_dir is None else pathlib.Path( copy_dir )
        self.verify_cert = verify_cert
        
    # ======================================================================

    def _retry_request( self, endpoint, data={}, files=None, isjson=True, downloadfile=None,
                        retries=5, sleeptime=2 ):
        """Send a request to the archive server with retries.

        endpoint - the part of the URL after self.url
        data - post data; a dict that will be json encoded by passing it to the json= argument of requests.post
        files - upload file info (passed to python requests with files=), or None (default)
        isjson - true if we expect a json response, false otherwise (default True)
        downloadfile - path of binary file to download, or None if none is expected (default None)
        retries - number of times to retry if there's a communications failure (default 5)
        sleeptime - time to sleep (in seconds) after a failure before retrying (default 2)
        verify - False if we don't bother verifying the certificate, true otherwise (default from config)

        If succesful, will return the data structure loaded from the
        returned json (if isjson is True) or True (if downloadfile is
        not None).

        If unsuccesful, will raise an exception.

        """
        
        url = f"{self.url}/{endpoint}"
        if ( not isjson ) and ( downloadfile is None ):
            raise RuntimeError( "isjson is false, and downloadfile is None... I don't know what to do with {url}" )
            
        countdown = retries
        while countdown >= 0:
            try:
                res = requests.post( f"{url}", data=data, files=files, verify=self.verify_cert )
                if res.status_code != 200:
                    raise RuntimeError( f"Got status_code={res.status_code} from {url} with data {data}" )
                if isjson:
                    if res.headers['content-type'] != 'application/json':
                        raise RuntimeError( f"Server returned {res.headers['content-type']}, expected json" )
                    resval = json.loads( res.text )
                    if "error" in resval:
                        raise RuntimeError( f"Got error response {resval['error']} from {url} with data {data}\n"
                                            f"{resval['traceback'] if 'traceback' in resval else '(No traceback)'}" )
                    return resval
                elif downloadfile is not None:
                    if res.headers['content-type'] != 'application/octet-stream':
                        raise RuntimeError( f"Server returned {res.headers['content-type']}, "
                                            f"expected an octet stream" )
                    with open( downloadfile, "wb" ) as ofp:
                        ofp.write( res.content )
                    return True
                else:
                    countdown = -1
                    raise RuntimeError( "This should never happen." )
            except Exception as e:
                countdown -= 1
                if countdown >= 0:
                    self.logger.warning( f"Exception trying {url} with data={data}; "
                                         f"will sleep {sleeptime}s and retry; "
                                         f"Exception: {str(e)}" )
                    try:
                        res.close()
                    except Exception as junk:
                        pass
                    time.sleep( sleeptime )
                else:
                    try:
                        res.close()
                    except Exception as junk:
                        pass
                    raise RuntimeError( f"Repeated exceptions trying archive url {url}" )

    
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

        if self.copy_dir is not None:
            destpath = self.copy_dir / serverpath
            if destpath.exists():
                if not destpath.is_file():
                    raise RuntimeError( f"Failed to copy to archive; {destpath} exists and isn't a normal file!" )
                if overwrite:
                    destpath.unlink()
                else:
                    raise RuntimeError( f"Failed to copy to archive; {destpath} exists and overwrite is False" )
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
            data = { "overwrite": overwrite,
                     "path": str(serverpath),
                     "dirmode": 0o755,
                     "mode": 0o644,
                     "token": self.token,
                     "md5sum": md5sum }
            ifp = open( localpath, "rb" )
            filedata = { "fileinfo": ifp }
            try:
                resval = self._retry_request( f"upload", data=data, files=filedata )
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

        if self.copy_dir is not None:
            archivepath = self.copy_dir / self.path_base / serverpath
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
            try:
                res = self._retry_request( "getfileinfo", data=data )
                return res
            except RuntimeError as ex:
                # OK, this is a bit ugly.  I'm going to parse the exception text,
                # to see if it's the special case of the file not found on the
                # server.  This requires internal knowledge of the server's
                # exception coding, which is unpleasant.  I considered adding
                # specific code for this to _retry_request, but that made that
                # routine (even) uglier, and really it's only stuff needed here.
                match = re.search( '^Got error response No such file', str(ex) )
                if match is not None:
                    return None
                raise ex

        raise RuntimeError( "This should never happen." )
            
    # ======================================================================

    def delete( self, serverpath, okifmissing=True ):
        """Delete a file in the archive

        serverpath - path of file to delete relative to self.path_base
        okifmissing - if False, then raise an exception if the file isn't there
        
        returns True if it thinks it worked, otherwise raises an exception
        """

        if self.copy_dir is not None:
            archivepath = self.copy_dir / self.path_base / serverpath
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
                     "overwrite": True,
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
        
        if self.copy_dir is not None:
            srcpath = pathlib.Path( self.copy_dir ) / serverpath
            if not srcpath.exists():
                raise FileNotFoundError( "Could not find archive file {serverpath}" )
            md5 = hashlib.md5()
            with open( srcpath, "rb" ) as ifp:
                md5.update( ifp.read() )
            md5sum = md5.hexdigest()

            if ( localmd5 is not None ) and ( localmd5 != md5sum ):
                if clobbermismatch:
                    localpath.unlink()
                else:
                    raise RuntimeError( "Local file {localpath} exists, but doesn't match md5sum expected "
                                        "from server.  Server md5sum: {md5sum}, local: {localmd5}" )

            # If we get this far and localfile exists, then we know we don't want to overwrite it
            if not localfile.exists():
                shutil.copy2( serverpath, localpath )
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
                                            f"{serverpath} on server; local={localmd5}, server={md5sum}" )

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

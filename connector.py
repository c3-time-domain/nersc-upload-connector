#!/usr/bin/python3
# -*- coding: utf-8 -*-

import sys
import os
import io
import re
import web
import json
import pathlib
import traceback
import logging
import hashlib

_logger = logging.getLogger(__name__)
if not _logger.hasHandlers():
    _logout = logging.StreamHandler( sys.stderr )
    _logger.addHandler( _logout )
    _formatter = logging.Formatter( f'[%(asctime)s - %(levelname)s] - %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S' )
    _logout.setFormatter( _formatter )
    _logger.setLevel( logging.INFO )


# ======================================================================

class Failure(Exception):
    def __init__( self, errormsg ):
        self.message = errormsg
        self.errorjson = json.dumps( { "status": "error", "error": errormsg} )

    def __str__( self ):
        return self.message

# ======================================================================

class UploadConnector(object):
    storage = pathlib.Path("/dest")
    
    def GET( self ):
        return self.do_the_things()

    def POST( self ):
        return self.do_the_things()

    def do_the_things( self ):
        web.header( 'Content-Type', 'text/html; charset="UTF-8"' )
        response = "<!DOCTYPE html>\n<html><head><title>NERSC Upload Connector</title></head>\n"
        response += "<body><h3>NERSC upload connector.</h3></body></html>\n"
        return response
    
    def init( self ):
        try:
            regex = re.compile( "^([^ ]+) *(.*)$" )
            pathtokens = {}
            with open("/run/secrets/connector_tokens") as ifp:
                lines = ifp.readlines()
                for line in lines:
                    line = line.strip()
                    match = regex.search( line )
                    if match is None:
                        _logger.warn( f"Failed to parse path/token line \"{line}\" )" )
                    else:
                        pathtokens[ match[1] ] = match[2]
            data = web.input( fileinfo={}, path=None, targetoflink=None, mode=None, dirmode=None,
                              overwrite=False, token=None )
            if data["path"] is None:
                raise Failure( "No file path specified" )
            ok = False
            for path, token in pathtokens.items():
                if data["path"][0:len(path)] == path:
                    if token != data["token"]:
                        _logger.error( f"Was passed token {data['token']} for path {data['path']}, "
                                       f"expected {token} for {path}" )
                        raise Failure( f"Invalid token for {data['path']}" )
                    else:
                        ok = True
                        break
            if not ok:
                raise Failure( f"File {data['path']} is not in a known path." )

            data["path"] = self.storage / data["path"]
            if data["targetoflink"] is not None:
                data["targetoflink"] = self.storage / data["targetoflink"]
            return data
        except Failure as e:
            raise e
        except Exception as e:
            raise Failure( f"Exception in UploadConnector.init: {str(e)}" )

    def mkdir( self, direc, dirmode=None ):
        if direc.is_dir():
            return
        if direc.exists() and ( not direc.is_dir() ):
            raise Failure( f'{str(direc)} exists and is not a directory.' )
        else:
            if dirmode is None:
                dirmode = 0o755
            direc.mkdir( parents=True, exist_ok=True )
            direc.chmod( int(dirmode) )
        
# ======================================================================

class GetFileInfo(UploadConnector):
    def do_the_things( self ):
        web.header( 'Content-Type', 'application/json' )
        try:
            data = self.init()
            if not data["path"].is_file():
                raise Failure( f'No such file {str(data["path"])}' )
            md5 = hashlib.md5()
            with open( data["path"], "rb" ) as ifp:
                md5.update( ifp.read() )
            stat = data["path"].stat()
            retval = { "serverpath": str(data["path"]),
                       "size": stat.st_size,
                       "md5sum": md5.hexdigest() }
            return json.dumps( retval )
        except Failure as ex:
            return ex.errorjson
        except Exception as ex:
            strerr = io.StringIO()
            traceback.print_exc( file=strerr )
            return json.dumps( { "status": "error",
                                 "error": f'Exception in GetFileInfo: {str(ex)}',
                                 "traceback": strerr.getvalue() } )
                
# ======================================================================

class DownloadFile(UploadConnector):
    def do_the_things( self ):
        try:
            data = self.init()
            if not data["path"].is_file():
                raise Failure( f'No such file {str(data["path"])}' )
            web.header( 'Content-Type', 'application/octet-stream' )
            web.header( 'Content-Disposition', f'attachment; filename="{data["path"].name}"' )
            with open( data["path"], "rb" ) as ifp:
                filedata = ifp.read()
            return filedata
        except Failure as ex:
            web.header( 'Content-Type', 'application/json' )
            return ex.errorjson
        except Exception as ex:
            web.header( 'Content-Type', 'application/json' )
            strerr = io.StringIO()
            traceback.print_exc( file=strerr )
            return json.dumps( { "status": "error",
                                 "error": f'Exception in DownloadFile: {str(ex)}',
                                 "traceback": strerr.getvalue() } )

# ======================================================================

class UploadFile(UploadConnector):
    def do_the_things( self ):
        web.header( 'Content-Type', 'application/json' )
        try:
            data = self.init()
            if (not data["overwrite"]) and data["path"].exists():
                raise Failure( f'File already exists: {str(data["path"])}' )
            self.mkdir( data["path"].parent, data["dirmode"] )
            with open(data["path"], "wb") as ofp:
                ofp.write( data["fileinfo"].value )
            if data["mode"] is not None:
                data["path"].chmod( int( data["mode"] ) )
            md5 = hashlib.md5()
            with open( data["path"], "rb" ) as ifp:
                md5.update( ifp.read() )
            md5sum = md5.hexdigest()
            if "md5sum" in data and data["md5sum"] is not None:
                if md5sum != data["md5sum"]:
                    data["path"].unlink()
                    raise Failure( f"md5sum of file {md5sum} doesn't match "
                                   f"passed md5sum {data['md5sum']}, file not written" )
            return json.dumps(
                {
                    "status": "File uploaded",
                    "filename": data["path"].name,
                    "path": str(data["path"]),
                    "length": len( data["fileinfo"].value ),
                    "md5sum": md5sum
                }
            )
        except Failure as ex:
            return ex.errorjson
        except Exception as ex:
            strerr = io.StringIO()
            traceback.print_exc( file=strerr )
            return json.dumps( { "status": "error",
                                 "error": f'Exception in UploadFile: {str(ex)}',
                                 "traceback": strerr.getvalue() } )

# ======================================================================

class DeleteFile(UploadConnector):
    def do_the_things( self ):
        web.header( 'Content-Type', 'application/json' )
        try:
            data = self.init()
            if not data["overwrite"]:
                raise Failure( f"Not deleting file, overwrite is False" )
            if not data["path"].exists():
                raise Failure( f"File doesn't exist: {str(data['path'])}" )
            if data["path"].is_dir():
                raise Failure( f"{str(data['path'])} is a directory" )
            data["path"].unlink()
            return json.dumps(
                {
                    "status": "File deleted",
                    "filename": data["path"].name,
                    "path": str(data["path"])
                }
            )
        except Failure as ex:
            return ex.errorjson
        except Exception as ex:
            strerr = io.StringIO()
            traceback.print_exc( file=strerr )
            return json.dumps( { "status": "error",
                                 "error": f'Exception in DeleteFile: {str(ex)}',
                                 "traceback": strerr.getvalue() } )


# ======================================================================

class MakeLink(UploadConnector):
    def do_the_things( self ):
        web.header( 'Content-Type', 'application/json' )
        try:
            data = self.init()
            if (not data["overwrite"]) and data["path"].exists():
                raise Failure( f'File already exists: {str(data["path"])}' )
            self.mkdir( data["path"].parent, data["dirmode"] )
            if not data["targetoflnk"].exists():
                raise Failure( f'Link target doesn\'t exist: {data["targetoflink"]}' )
            data["path"].symlink_to( data["targetoflink"] )
            return json.dumps(
                {
                    "status": "Link created",
                    "target": str(data["targetoflink"]),
                    "link": str(data["path"])
                }
            )
        except Failure as ex:
            return ex.errorjson
        except Exception as ex:
            strerr = io.StringIO()
            traceback.print_exc( file=strerr )
            return json.dumps( { "error": f'Exception in MakeLink: {str(ex)}',
                                 "traceback": strerr.getvalue() } )
            
# ======================================================================

urls = ( "/upload", "UploadFile",
         "/getfileinfo", "GetFileInfo",
         "/download", "DownloadFile",
         "/makelink", "MakeLink",
         "/delete", "DeleteFile",
         "/", "UploadConnector"
         )
web.config.session_parameters["samesite"] = "lax"
app = web.application(urls, locals())
application = app.wsgifunc()

if __name__ == "__main__":
    app.run()

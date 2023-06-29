import sys
import pathlib
import pytest
import hashlib
import random

_rundir = pathlib.Path(__file__).parent
if not str( _rundir.parent ) in sys.path:
    sys.path.insert( 0, str( _rundir.parent ) )
from archive import Archive

class TestArchive:
    @pytest.fixture(scope='class')
    def tokens( self ):
        tokens = {}
        with open( "/run/secrets/connector_tokens" ) as ifp:
            for line in ifp:
                them = line.strip().split()
                tokens[them[0]] = them[1]
        return tokens
                
    @pytest.fixture(scope='class')
    def archive( self, tokens ):
        return Archive( archive_url='http://archive-server:8080/',
                        path_base='test1',
                        token=tokens['test1/'],
                        verify_cert=False,
                        copy_dir=None )

    @pytest.fixture(scope='class')
    def localfile( self ):
        contents = "".join( random.choices( '0123456789abcdef', k=16 ) )
        filepath = pathlib.Path( "/tmp/test1" ) / ( "".join( random.choices( '0123456789abcdef', k=16  ) ) )
        filepath.parent.mkdir( parents=True, exist_ok=True )
        with open( filepath, "w" ) as ofp:
            ofp.write( contents )
        md5 = hashlib.md5()
        md5.update( contents.encode("ascii") )
        yield contents, filepath, md5.hexdigest()
        filepath.unlink()

    @pytest.fixture(scope='class')
    def upload( self, archive, localfile ):
        contents, localpath, localmd5 = localfile
        serverpath = pathlib.Path( "thing" ) / localpath.name
        yield archive.upload( localpath, "thing", localpath.name )
        archive.delete( serverpath )


    def test_upload( self, localfile, upload ):
        contents, filepath, md5sum = localfile
        assert md5sum == upload

    
    def test_getinfo( self, archive, localfile, upload ):
        contents, filepath, md5sum = localfile
        serverpath = pathlib.Path( "thing" ) / filepath.name
        info = archive.get_info( serverpath )
        assert info["serverpath"] == f"/dest/test1/thing/{filepath.name}"
        assert info["size"] == 16
        assert info["md5sum"] == md5sum


    def test_download( self, archive, localfile, upload ):
        contents, filepath, md5sum = localfile
        serverpath = pathlib.Path( "thing" ) / filepath.name
        localpath = pathlib.Path( "/tmp/downloaded/thing" ) / filepath.name
        localpath.parent.mkdir( exist_ok=True, parents=True )
        archive.download( serverpath, localpath )
        assert localpath.is_file()
        md5 = hashlib.md5()
        with open( localpath, "rb" ) as ifp:
            md5.update( ifp.read() )
        assert md5.hexdigest() == md5sum

        localpath.unlink()
        

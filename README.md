# A system for archiving files to either a local cache directory, or a remot server

The `Dockerfile` in the top level builds an image that is used as an archive connector, designed for deployment at NERSC Spin, but that could conceivably work on any appropriate web server.  The actual webserver code is in `connector.py`.

Client-side, use `archive.py`.

To test:

'''
cd tests
docker compose build
docker compose run runtests
docker compose down -v
'''

Irritatingly, `docker compose down -v` doesn't seem to fully clean up after itself.  You may need to do `docker ps -a`, manually `docker rm` a vestigal container, and then do `docker compose down -v` again.



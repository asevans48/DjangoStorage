"""
WEB Dav implementation for local file storage or RAID

@author aevans
"""

import os

from django.core.cache import cache, caches
from django.core.files import File
from django.core.files.storage import FileSystemStorage

from storage.webdav import ResumableWebDav


class LocalFileStorage(FileSystemStorage, ResumableWebDav):

    def __init__(self, location=None,
                 base_url=None,
                 file_permissions_mode=None,
                 directory_permissions_mode=None):
        FileSystemStorage.__init__(self, location, base_url,
                                        file_permissions_mode,
                                        directory_permissions_mode)
        self.location = location

    def join_path(self, path_parts):
        """
        Join a path list
        :param path_parts:  The path parts
        :type path_parts:  dir
        :return:  The path string
        :rtype:  str
        """
        return os.path.sep.join(path_parts)

    def save(self, name, content, max_length=None):
        """
        Save new content to the file specified by name. The content should be
        a proper File object or any python file-like object, ready to be read
        from the beginning.
        """
        # Get the proper name for the file, as it will actually be saved.
        if name is None:
            name = content.name
        full_path = self.path(name)
        rval = None
        if not hasattr(content, 'chunks'):
            with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                    name = self.get_available_name(name, max_length=max_length)
                    if type(content) is not bytes:
                        contento = File(content, name)
                        rval = self._save(name, contento)
                    else:
                        with open(self.path(name), 'wb') as fp:
                            fp.write(content)
        return rval

    def move(self, name, new_name):
        """
        Move (rename a file)
        :param name:  current name of the file
        :type name:  str
        :param new_name:  New name of the file
        :type new_name:  str
        :return whether or not the file moved
        :rtype:  boolean
        """
        full_path = self.path(name)
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                new_path = self.path(new_name)
                os.rename(full_path, new_path)
                return True
        return False

    def append(self, name, content, user):
        """
        The resumable part
        :param name:  Name of the file
        :type name:  str
        :param content:  The content to write
        :type content:  str
        :param user:  user writing to the file
        :type user:  str
        :return whether the data was appended
        :rtype: boolean
        """
        full_path = self.path(name)
        if self.is_locked(full_path) is False or cache.get(full_path) is not user:
            with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                    self.lock(full_path, user)
                    try:
                        if type(content) is str:
                            with open(full_path, 'a') as fp:
                                fp.write(content)
                        else:
                            with open(full_path, 'ab') as fp:
                                fp.write(content)
                    finally:
                        self.unlock(full_path, user)
                    return True
        return False

    def mkcollection(self, name, user):
        """
        Make a collection
        :param name:  The name of the file
        :type name:  str
        :param user:  The user making the collection
        :type user:  str
        :return:  Whether the file was created
        :rtype: boolean
        """
        full_path = self.path(name)
        if self.is_locked(full_path) is False:
            if self.exists(name) is False:
                with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                    self.lock(full_path, user)
                    try:
                        fd = os.mknod(full_path)
                    finally:
                        self.unlock(full_path, user)
                    return True
        return False

    def propfind(self, name):
        """
        Get file properties
        :param name:  The name of the file
        :type name:  str
        :return:  File information
        :rtype:  dict
        """
        full_path = self.path(name)
        finf = {}
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                finf['modified_time'] = self.get_modified_time(name)
                finf['accessed_time'] = self.get_accessed_time(name)
                finf['created_time'] = self.get_created_time(name)
                finf['valid_name'] = self.get_valid_name(name)
                finf['path'] = full_path
                fd = os.open(full_path, os.O_RDONLY)
                finf['fstat'] = os.fstat(fd)
        return finf

    def delete(self, name):
        """
        Delete the file.

        :param name:  The name of the file
        :type name:  str
        :return:  Whether the file was deleted
        :rtype:  boolean
        """
        full_path = self.path(name)
        if os.path.exists(full_path):
            with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                    os.remove(full_path)
                    return True
        return False


    def safe_read(self, name):
        """
        Read the file in a threadsafe manner

        :param name:  The name of the file
        :type name:  str
        :return:  The file contents or None if there is no content
        :rtype: str
        """
        full_path = self.path(name)
        content = None
        if self.exists(name):
            with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                with open(full_path, 'r') as fd:
                    content = fd.read()
        return content

    def safe_read_chunk(self, name, offset=0, length=1024):
        """
        Safe read a chunk
        :param name:  The name of the file
        :type name:  str
        :param offset:  The offset in bytes to start reading from
        :type offset:  int
        :param length:  The number of bytes to read
        :type length:  int
        :return:  The bytes and number of bytes read
        :rtype: tuple
        """
        full_path = self.path(name)
        byte_buffer = None
        l = 0
        if self.exists(name):
            with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                fd = os.open(full_path, 'r')
                try:
                    fsize = os.path.getsize(full_path)
                    if offset < fsize:
                        rlength = fsize - offset
                        l = length
                        if rlength < length:
                            l = rlength
                        fd.lseek(offset)
                        byte_buffer = fd.read(l)
                finally:
                    fd.close()
        return (byte_buffer, l)

    def download(self, name, max_buf_length):
        """
        Create an iterator for downloading from a file (lazy read).
        This is useful for rtc.

        :param name:  The name of the file
        :type name:  str
        :param max_buf_length:  Maximum length of the buffer to read
        :type max_buf_length:  int
        :return:  next bytes and number of read bytes with the current offset
        :rtype:  tuple
        """
        full_path = self.path(name)
        fsize = os.path.getsize(full_path)
        current_offset = 0
        while current_offset < fsize:
            buf, l = self.safe_read_chunk(name, current_offset, max_buf_length)
            current_offset += l
            yield (buf, l, current_offset)

    def mkdirs(self, dirs):
        """
        Create a full set of directories as needed
        :param dirs:  The path to create
        :return:  Whether the director was created
        :rtype:  bool
        """
        dirs = os.path.sep.join([self.location, dirs])
        if os.path.exists(dirs):
            return False
        os.mkdir(dirs)
        return True

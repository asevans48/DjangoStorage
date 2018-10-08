"""
WEBDAV based API for Gluster FS storage

@author aevans
"""
import os
import traceback
from urllib.parse import urljoin

from django.conf import global_settings as settings
from django.core.cache import cache, caches
from django.core.files.move import file_move_safe
from django.core.files.storage import Storage
from django.core.signals import setting_changed
from django.utils._os import safe_join
from django.utils.encoding import filepath_to_uri
from django.utils.functional import cached_property
from gluster import gfapi

from filestorage.filestorageapp.storage.webdav import ResumableWebDav


class GlusterFSConfig(object):

    def __init__(self, host, port, volume, proto=u'tcp', log_file=u'/dev/null',\
                 log_level=7):
        self.host = host
        self.port = port
        self.proto = proto
        self.volume = volume
        self.log_file = log_file
        self.log_level = log_level


class GlusterFSStorage(Storage, ResumableWebDav):

    def __init__(self, fs_config, option=None, location=None, base_url=None, file_permissions_mode=None,
                 directory_permissions_mode=None):
        """
        Constructor

        :param fs_config:  The file system config
        :type fs_config:  GlusterFSConfig
        :param option:  File system Options
        :type option:  dict
        """
        super(GlusterFSStorage, self).__init__()
        if not option:
            option = settings.CUSTOM_STORAGE_OPTIONS
        self.__volume = gfapi.Volume(fs_config.host, fs_config.volume, proto=fs_config.proto,\
                              port=fs_config.port, log_file=fs_config.log_file,\
                              log_level=fs_config.log_level)
        if self.__volume.mounted is False:
            self.__volume.mount()
        self._location = location
        self._base_url = base_url
        self._file_permissions_mode = file_permissions_mode
        self._directory_permissions_mode = directory_permissions_mode
        setting_changed.connect(self._clear_cached_properties)

    def _clear_cached_properties(self, setting, **kwargs):
        """Reset setting based property values."""
        if setting == 'MEDIA_ROOT':
            self.__dict__.pop('base_location', None)
            self.__dict__.pop('location', None)
        elif setting == 'MEDIA_URL':
            self.__dict__.pop('base_url', None)
        elif setting == 'FILE_UPLOAD_PERMISSIONS':
            self.__dict__.pop('file_permissions_mode', None)
        elif setting == 'FILE_UPLOAD_DIRECTORY_PERMISSIONS':
            self.__dict__.pop('directory_permissions_mode', None)

    def _value_or_setting(self, value, setting):
        return setting if value is None else value

    @cached_property
    def base_location(self):
        return self._value_or_setting(self._location, settings.MEDIA_ROOT)

    @cached_property
    def location(self):
        return os.path.abspath(self.base_location)

    @cached_property
    def base_url(self):
        if self._base_url is not None and not self._base_url.endswith('/'):
            self._base_url += '/'
        return self._value_or_setting(self._base_url, settings.MEDIA_URL)

    @cached_property
    def file_permissions_mode(self):
        return self._value_or_setting(self._file_permissions_mode, settings.FILE_UPLOAD_PERMISSIONS)

    @cached_property
    def directory_permissions_mode(self):
        return self._value_or_setting(self._directory_permissions_mode, settings.FILE_UPLOAD_DIRECTORY_PERMISSIONS)

    def path(self, name):
        return safe_join(self.location, name)

    def url(self, name):
        if self.base_url is None:
            raise ValueError("This file is not accessible via a URL.")
        url = filepath_to_uri(name)
        if url is not None:
            url = url.lstrip('/')
        return urljoin(self.base_url, url)

    def _save(self, name, content):
        """
        Save a file to the gluster fs.

        :param name:  The name of the file
        :type name:  str
        :param content:  The content to save
        :type content:  dict
        :return:  The name of the file
        """
        full_path = self.path(name)
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                if cache.islocked(full_path) is False:
                    with cache.lock(full_path):
                        cache.set(full_path, 'storage')
                    try:
                        directory = os.path.dirname(full_path)

                        # Create any intermediate directories that do not exist.
                        if self.__volume.exists(directory) is False:
                            try:
                                if self.directory_permissions_mode is not None:
                                    # os.makedirs applies the global umask, so we reset it,
                                    # for consistency with file_permissions_mode behavior.
                                    self.volume.makedirs(directory, self.directory_permissions_mode)
                                else:
                                    self.volume.makedirs(directory)
                            except FileNotFoundError:
                                # There's a race between os.path.exists() and os.makedirs().
                                # If os.makedirs() fails with FileNotFoundError, the directory
                                # was created concurrently.
                                pass
                        if not os.path.isdir(directory):
                            raise IOError("%s exists and is not a directory." % directory)

                        # There's a potential race condition between get_available_name and
                        # saving the file; it's possible that two threads might return the
                        # same name, at which point all sorts of fun happens. So we need to
                        # try to create the file, but if it already exists we have to go back
                        # to get_available_name() and try again.

                        while True:
                            try:
                                # This file has a file path that we can move.
                                if hasattr(content, 'temporary_file_path'):
                                    file_move_safe(content.temporary_file_path(), full_path)

                                # This is a normal uploadedfile that we can stream.
                                else:
                                    # The current umask value is masked out by os.open!
                                    fd = self.__volume.open(full_path, self.OS_OPEN_FLAGS, 0o666)
                                    _file = None
                                    try:
                                        for chunk in content.chunks():
                                            if _file is None:
                                                _file = fd.dup()
                                            _file.write(chunk)
                                    finally:
                                        if _file is not None:
                                            _file.close()
                                        fd.close()
                            except FileExistsError:
                                # A new name is needed if the file exists.
                                name = self.get_available_name(name)
                                full_path = self.path(name)
                            else:
                                # OK, the file save worked. Break out of the loop.
                                break

                        if self.file_permissions_mode is not None:
                            self.__volume.chmod(full_path, self.file_permissions_mode)
                    finally:
                        cache.delete(full_path)
                    # Store filenames with forward slashes, even on Windows.
                    return (True, name.replace('\\', '/'))
        return (False, cache.get(full_path))

    def delete(self, name, user):
        """
        Delete a file.

        :param name:  The name of the file
        :type name:  str
        :param user:  The user creating the file
        :type user:  str
        :return: Whether the file could be deleted
        :rtype:  boolean
        """
        full_path = self.path(name)
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                if self.is_locked(full_path) is False:
                    self.lock(full_path, user)
                    try:
                        if self.__volume.exists(full_path):
                            self.__volume.remove(full_path)
                    finally:
                        self.unlock(full_path, user)
                    return True
        return False

    def exists(self, name):
        """
        Check whether a file exists

        :param name:  The name of the file
        :type name:  str
        :return:  Whether the path exists
        :rtype:  boolean
        """
        full_path = self.path(name)
        return self.__volume.exists(full_path)

    def get_dirs(self, path, files, dirs):
        files = []
        dirs = []
        entries = self.__volume.listdir(path)
        if entries:
            for entry in entries:
                path = '/'.join([path, entry])
                if self.__volume.isfile(path):
                    files.append(entry)
                elif self.__volume.isdir(path):
                    dirs.append(entry)
        return (dirs, files)

    def listdir(self, path):
        """
        List a directory at the given path
        :param path:  The path to list
        :type: path:  str
        :return:  A tuple of directories and files
        :rtype:  tuple
        """
        if self.__volume.isdir(path):
            return self.get_dirs(path)
        return ([],[])

    def size(self, name):
        """
        Get the size of the file or directory
        :param name:  The file to check
        :type name:  str
        :return:  The size as long
        :rtype:  long
        """
        full_path = self.path(name)
        return self.__volume.getsize(full_path)

    def url(self, name):
        """
        Get a path to the file in glusterfs, not an accessible URL
        :param name:  The name of the file
        :type: name:  str
        :return:  The url
        :rtype:  str
        """
        return self.path(name)

    def get_accessed_time(self, name):
        """
        Return the last accessed time (as a datetime) of the file specified by
        name. The datetime will be timezone-aware if USE_TZ=True.

        :param name:  The name of the file
        :type name:  str
        :return:  The last access time
        :rtype:  datetime.datetime
        """
        full_path = self.path(name)
        return self.__volume.getatime(full_path)

    def get_created_time(self, name):
        """
        Return the creation time (as a datetime) of the file specified by name.
        The datetime will be timezone-aware if USE_TZ=True.

        :param name:  The name of the file
        :type name:  str
        :return:  The last change time
        :rtype:  datetime.datetime
        """
        full_path = self.path(name)
        return self.__volume.getctime(full_path)

    def get_modified_time(self, name):
        """
        Return the last modified time (as a datetime) of the file specified by
        name. The datetime will be timezone-aware if USE_TZ=True.

        :param name:  The name of the file
        :type name:  str
        :return:  The last modified time
        :rtype:  datetime.datetime
        """
        full_path = self.path(name)
        return self.__volume.getmtime(full_path)

    def move(self, name, new_name, user):
        """
        Move (rename a file)
        :param name:  current name of the file
        :type name:  str
        :param new_name:  New name of the file
        :type new_name:  str
        :param user:  The user creating the file
        :type user:  str
        """
        full_path = self.path(name)
        new_path = self.path(new_name)
        if self.is_locked(full_path):
            self.lock(full_path, user)
            try:
                self.__volume.copy2(full_path, new_path)
            finally:
                self.unlock(full_path, user)

    def append(self, name, content, user):
        """
        The resumable part
        :param name:  Name of the file
        :type name:  str
        :param content:  The content to write
        :type content:  str
        :param user:  The user creating the file
        :type user:  str
        :return: Whether writing was successful
        :rtype: boolean
        """
        full_path = self.path(name)
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                if self.is_locked(full_path) is False:
                    self.lock(full_path, user)
                    try:
                        with self.__volume.fopen(full_path, 'a+') as fp:
                            fp.write(content)
                    finally:
                        self.unlock(full_path, user)
                    return True
        return False

    def mkcollection(self, name, user):
        """
        Make a collecotion
        :param name:  The name of the file
        :type name:  str
        :param user:  The user creating the file
        :type user:  str
        :return: Whether the file was created and any error
        :rtype:  tuple
        """
        full_path = self.path(name)
        if self.exists(name) is False:
            try:
                with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
                    with caches['default'].lock('{}_{}'.format(full_path, 'writer')):
                        if self.is_locked(full_path) is False:
                            self.lock(full_path, user)
                            try:
                                self.__volume.mknod(full_path)
                            finally:
                                self.unlock(full_path, user)
                return (True, None)
            except Exception as e:
                exception = traceback.format_exc()
                return (False, exception)
        return (False, None)

    def propfind(self, name):
        """
        Get file properties
        :param name:  The name of the file
        :type name:  str
        :return:  File information
        :rtype:  gfapi.api.Stat
        """
        full_path = self.path(name)
        with caches['default'].lock('{}_{}'.format(full_path, 'reader')):
            fd = self.__volume.open(full_path)
            stat = fd.fstat()
            fd.close()
        return stat

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
                fd = self._volume.open(full_path)
                try:
                    content = fd.read()
                finally:
                    fd.close()
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
                fd = self.__volume.fopen(full_path, 'r')
                try:
                    fsize = self.__volume.getsize(full_path)
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
        fsize = self.__volume.getsize(full_path)
        current_offset = 0
        while current_offset < fsize:
            buf, l = self.safe_read_chunk(name, current_offset, max_buf_length)
            current_offset += l
            yield (buf, l, current_offset)

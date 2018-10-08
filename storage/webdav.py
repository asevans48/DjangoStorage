"""
Abstract webdav object

@auhtor aevans
"""

from django.core.cache import cache, caches

from django.conf import global_settings as settings


class ResumableWebDav(object):

    def is_locked(self, fpath):
        """
        Check whether a path is locked

        :param fpath:  The path to the file
        :type fpath:  str
        :return:  Whether the resource is locked
        :rtype:  boolean
        """
        return cache.get(fpath) is not None

    def lock(self, fpath, user):
        """
        Lock a file
        :param fpath:  The full file path
        :type fpath:  str
        :param user:  The user performing the locking
        :type user:  str
        :return: Whether the resource was locked and the user
        :rtype:  tuple
        """

        if caches['default'].get(fpath):
            cache.set(fpath, user, settings.MAX_FILE_LOCK_SECONDS)
            return (True, user)
        else:
            user = cache.get(fpath)
            return (False, user)

    def unlock(self, fpath, user):
        """
        Lock a file
        :param fpath:  Path to the file
        :type fpath:  str
        :param user:  The user to check against
        :type user:  The
        """
        check_user = cache.get(fpath)
        if check_user and check_user == user:
            cache.delete(fpath)
        return check_user

    def move(self, name, new_name):
        """
        Move (rename a file)
        :param name:  current name of the file
        :type name:  str
        :param new_name:  New name of the file
        :type new_name:  str
        """

        pass

    def append(self, name, content):
        """
        The resumable part
        :param name:  Name of the file
        :type name:  str
        :param content:  The content to write
        :type content:  str
        :param max_length:  The maximum file length
        :type max_length:  int
        """
        pass

    def mkcollection(self, name):
        """
        Make a collecotion
        :param name:  The name of the file
        :type name:  str
        """
        pass

    def propfind(self, name):
        """
        Get file properties
        :param name:  The name of the file
        :type name:  str
        :return:  File information
        :rtype:  dict
        """
        pass

    def safe_read(self, name):
        """
        Safely read a file

        :param name:  The name of the file
        :type name:  str
        :return:  The content
        :rtype:  object
        """
        pass

    def safe_read_chunk(self, name, offset, length):
        """
        Safely read a chunk

        :param name:  The name of the file
        :type name:  str
        :param offset:  The starting file offset
        :type offset:  int
        :param length:  The length of the buffer to read
        :type length:  int
        :return:  The buffer read
        :rtype: bytes
        """
        pass

    def download(self, name, max_buf_length):
        """
        Safely iterate through a file in chunks. Useful for
        WebRTC

        :param name:  The name of the file
        :type name:  str
        :param max_buf_length:  Maximum buffer length
        :type max_buf_length:  int
        :return:  the next bytes, number of bytes read, current offset
        :rtype:  int
        """
        pass

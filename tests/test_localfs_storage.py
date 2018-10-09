"""
Tests for the LocalFileStorage class.

@author aevans
"""
import datetime
import os

from django.conf import settings
from django.core.cache import cache, caches
from django.core.files.base import ContentFile

from ..storage.localfs_storage import LocalFileStorage

import pytest

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'filestorage.filestorage.settings')
print(caches['default'])


@pytest.mark.order1
def test_locking():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    lfs.lock(lfs.path('test_app.txt'), 'me')
    assert(cache.get(lfs.path('test_app.txt')) == 'me')
    lfs.unlock(lfs.path('test_app.txt'), 'you')
    assert (cache.get(lfs.path('test_app.txt')) == 'me')
    lfs.unlock(lfs.path('test_app.txt'), 'me')
    assert(cache.get(lfs.path('test_app.txt')) is None)


@pytest.mark.order2
def test_save_file():
    if os.path.exists('~/docs/') is False:
        os.mkdir('~/docs/')
    if os.path.exists('~/docs/test_app.txt'):
        os.remove('~/docs/test_app.txt')
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    lfs.save('test_app.txt', ContentFile('hello world!'))
    assert(os.path.exists('~/docs/test_app.txt'))
    with open('~/docs/test_app.txt', 'r') as fp:
        lines = fp.read()
        assert('hello world!' in lines)


@pytest.mark.order3
def test_save_file_to_new_dir():
    if os.path.exists('/home/aevans/docs/testdir/'):
        os.remove('/home/aevans/docs/testdir/')
    lfs = LocalFileStorage(location='/home/aevans/docs/testdir/')
    lfs.save('test_app.txt', ContentFile('hello world!'))
    assert (os.path.exists(lfs.path('test_app.txt')))
    with open(lfs.path('test_app.txt'), 'r') as fp:
        lines = fp.read()
        assert ('hello world!' in lines)


@pytest.mark.order4
def test_get_accessed_time():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    dt = lfs.get_accessed_time('test_app.txt')
    assert(type(dt) is datetime.datetime)
    assert(dt < datetime.datetime.now())


@pytest.mark.order5
def test_get_modified_time():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    dt = lfs.get_modified_time('test_app.txt')
    assert (type(dt) is datetime.datetime)
    assert (dt < datetime.datetime.now())


@pytest.mark.order6
def test_get_created_time():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    dt = lfs.get_created_time('test_app.txt')
    assert (type(dt) is datetime.datetime)
    assert (dt < datetime.datetime.now())


@pytest.mark.order7
def test_move_file():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    lfs.move('test_app.txt', 'test_app2.txt')
    assert(os.path.exists('~/docs/test_app.txt') is False)
    assert(os.path.exists('~/docs/test_app2.txt') is True)
    os.rename('~/docs/test_app2.txt', '~/docs/test_app.txt')
    assert(os.path.exists('~/docs/test_app.txt'))


@pytest.mark.order8
def test_append_to_file():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    lfs.append('test_app.txt', ' it is me!', 'me')
    with open('/home/aevans/docs/test_app.txt', 'r') as fp:
        lines = fp.read()
        assert('it is me!' in lines)
        assert('hello world' in lines)


@pytest.mark.order9
def test_mkcollection():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    lfs.mkcollection('test_app3.txt', 'me')
    assert(os.path.exists(lfs.path('test_app3.txt')))
    os.remove(lfs.path('test_app3.txt'))


@pytest.mark.order10
def test_propfind():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    stat = lfs.propfind(lfs.path('test_app.txt'))
    assert(stat.get('modified_time') is not None)


@pytest.mark.order11
def test_append_bytes():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    byteo = 'hello world'.encode('utf-8')
    if lfs.exists('test_bytes.txt'):
        lfs.delete('test_bytes.txt')
    lfs.append('test_bytes.txt', byteo, 'me')
    lpath = lfs.path('text_bytes.txt')
    assert (os.path.exists(lpath))
    assert(os.path.getsize(lpath) > 0)


@pytest.mark.order12
def test_save_bytes():
    lfs = LocalFileStorage(location='/home/aevans/docs/')
    byteo = 'hello world'.encode('utf-8')
    if lfs.exists('test_bytes.txt'):
        lfs.delete('test_bytes.txt')
    print('SAVING')
    lfs.save('test_bytes.txt', byteo)
    lpath = lfs.path('text_bytes.txt')
    assert(os.path.exists(lpath))
    assert(os.path.getsize(lpath) > 0)

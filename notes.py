#!/usr/bin/env python
# -*- coding: utf-8 -*-
import errno
import fuse
import stat
import time
import sqlite3
import StringIO
import functools
import re
import posixpath

fuse.fuse_python_api = (0, 2)

class DatabaseObject:

    def __init__(self, db):
        self._db = db

    def _query(self, query, *args):
        con = sqlite3.connect(self._db)
        con.row_factory = sqlite3.Row
        cur = con.cursor()
        cur.execute(query, args)
        rows = cur.fetchall()
        con.commit()
        con.close()
        return rows

def cached(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if self._psync is None or (time.time() - self._psync > 10):
            self._synchronize()
        return f(self, *args, **kwargs)
    return wrapper

def synchronized(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        self._synchronize()
        return f(self, *args, **kwargs)
    return wrapper

class Note(DatabaseObject):

    F = re.compile(r'^/(\d+)([^/]+)?\.txt$')

    # FIXME: length() doesn't work with bytes.

    SQL = {
    'note':
#    '''
#        select
#                ZNOTE.Z_PK                  as
#            id,
#                ZNOTE.ZTITLE                as
#            title,
#                ZNOTE.ZAUTHOR               as
#            author,
#                ZNOTE.ZSUMMARY              as
#            summary,
#
#                ZNOTE.ZBODY                 as
#            body_id,
#                ZNOTEBODY.ZCONTENT          as
#            body,
#                length(ZNOTEBODY.ZCONTENT)  as
#            body_size,
#                ZNOTE.ZCREATIONDATE         as
#            itime,
#                ZNOTE.ZMODIFICATIONDATE     as
#            mtime,
#
#                ZNOTE.ZDELETEDFLAG          as
#            deleted
#
#        from
#            ZNOTE
#        left join
#            ZNOTEBODY
#        on
#            ZNOTE.ZBODY = ZNOTEBODY.Z_PK
#        where
#            ZNOTE.Z_PK = ?
#        ;
#    ''',

    '''
        select
                Note.ROWID                  as
            id,
                Note.title                  as
            title,
                Note.author                 as
            author,
                Note.summary                as
            summary,

                note_bodies.note_id         as
            body_id,
                note_bodies.data            as
            body,
                length(note_bodies.data)    as
            body_size,
                Note.creation_date          as
            itime,
                Note.modification_date      as
            mtime

        from
            Note 
        left join
            note_bodies
        on
            Note.ROWID = note_bodies.note_id
        where
            NOTE.ROWID = ?
        ;
    ''',


    'write_body':
#    '''
#        update
#            ZNOTEBODY
#        set
#            ZCONTENT = ?
#        where
#            ZNOTEBODY.Z_PK = (
#                select
#                    ZNOTE.ZBODY
#                from
#                    ZNOTE
#                where
#                    ZNOTE.Z_PK = ?
#            )
#        ;
#    ''',
    '''
        update
            note_bodies
        set
            note_bodies.data = ?
        where
            note_bodies.note_id = Note.ROWID
        ;
    ''',

    'truncate_body':
    '''
        update
            note_bodies
        set
            data = ?
        where
            note_id = (
                select
                    ROWID
                from
                    Note
                where
                    Note.title = ?
            )
        ;
    ''',

    'create_note':
    '''
        insert into
            ZNOTE
        (
        )
        values
    ''',
    }


    def __init__(self, db, nid=None):
        DatabaseObject.__init__(self, db)
        if nid is None:
            # Create a new one. FIXME
            raise Exception('Note does not exist')
        self._id = nid
        self._psync = time.time()

    def _synchronize(self):
        try:
            (
                self._id,
                self._title,
                self._author,
                self._summary,
                self._body_id,
                self._body,
                self._body_size,
                self._itime,
                self._mtime,
                #self._deleted,
                    ) = self._query(self.SQL['note'], self._id)[0]
        except IndexError:
            raise Exception('Note does not exist')
        self._psync = None

    def get_id(self):
        return self._id


    @synchronized
    def read_body(self, offset, length):
        return str(self._body.encode('utf-8'))[offset:length]

    @synchronized
    def write_body(self, buf, offset):
        body = StringIO.StringIO(self._body)
        body.seek(offset)
        body.write(buf)
        self._query(self.SQL['write_body'], body.getvalue(), self._id)
        return len(buf)

    @synchronized
    def truncate_body(self, size):
        body = StringIO.StringIO(self._body)
        body.truncate(size)
        self._query(self.SQL['write_body'], body.getvalue(), self._id)


    @cached
    def get_size(self):
        return self._body_size

    @cached
    def get_mtime(self):
        return self._mtime

    @cached
    def get_deleted(self):
        return self._deleted


    @synchronized
    def get_filename(self):
        return '{id:04d}-{title}.txt'.format(
                id=self._id,
                title=self._title.encode('utf-8')
                )

    @staticmethod
    def parse_path(path):
        ''' "/" <id> "." "txt" '''
        path = posixpath.normpath(path.lower())

        m = Note.F.match(path)
        if m is not None:
            return int(m.group(1))
        
        raise Exception(path)


class NoteCollection(DatabaseObject):

    SQL = {
    'notes':
#    '''
#        select
#            Z_PK
#        from
#            ZNOTE
#        ;
#    ''',
    '''
        select
            ROWID
        from
            note
        ;
    ''',
    'notes_full':
    '''
        select
                Note.ROWID                  as
            id,
                Note.title                  as
            title,
                Note.author                 as
            author,
                Note.summary                as
            summary,

                note_bodies.note_id         as
            body_id,
                note_bodies.data            as
            body,
                length(note_bodies.data)    as
            body_size,
                Note.creation_date          as
            itime,
                Note.modification_date      as
            mtime

        from
            Note 
        left join
            note_bodies
        on
            Note.ROWID = note_bodies.note_id
        ;
    ''',
    }

    def __init__(self, db):
        DatabaseObject.__init__(self, db)
        self._notes = {}
        self._psync = None

    def _synchronize(self):
        self._notes = dict(((nid, Note(self._db, nid))
                for (nid,) in self._query(self.SQL['notes'])))
        for note in self._query(self.SQL['notes_full']):
            try:
                (
                    self._notes[note[0]]._id,
                    self._notes[note[0]]._title,
                    self._notes[note[0]]._author,
                    self._notes[note[0]]._summary,
                    self._notes[note[0]]._body_id,
                    self._notes[note[0]]._body,
                    self._notes[note[0]]._body_size,
                    self._notes[note[0]]._itime,
                    self._notes[note[0]]._mtime,
                    #self._deleted,
                        ) = note
            except Exception:
                raise
        self._psync = time.time()


    @cached
    def __getitem__(self, key):
        return self._notes[key]

    @cached
    def __iter__(self):
        return self._notes.itervalues()


class NotesFS(fuse.Fuse):

    def __init__(self, db, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        self._db    = db
        self._notes = NoteCollection(db)

    # TODO: implement
    def setattr(self, path, x, y):
        pass

    def getattr(self, path):
        st = fuse.Stat()

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0755
            st.st_nlink = 2
            st.st_atime = int(time.time())
            st.st_mtime = st.st_atime
            st.st_ctime = st.st_atime
        else:
            try:
                note = self._notes[Note.parse_path(path)]
                st.st_size  = note.get_size()
                st.st_mode  = stat.S_IFREG | 0666
                #st.st_nlink = int(note.get_deleted())
                st.st_nlink = 1
                st.st_atime = int(time.time())
                st.st_mtime = int(note.get_mtime())
                st.st_ctime = int(note.get_mtime())
            except:
                raise
                return -errno.ENOENT

        return st

    def readdir(self, path, offset):
        direntries = ['.', '..']

        if path != '/':
            yield - errno.ENOENT

        self._notes._synchronize()

        direntries.extend([n.get_filename()
            for n in self._notes])

        for d in direntries:
            yield fuse.Direntry(d)

    def open(self, path, flags):
        pass

    def read(self, path, length, offset):
        note = Note(self._db, Note.parse_path(path))
        return note.read_body(offset, length)

    def write(self, path, buf, offset):
        note = Note(self._db, Note.parse_path(path))
        return note.write_body(buf, offset)

    def truncate(self, path, size):
        note = Note(self._db, Note.parse_path(path))
        return note.truncate_body(size)

    # TODO: implement. Make non-direct I/O?
    def flush(self, path):
        pass

    # TODO: implement
    def getxattr(self, path, a, b):
        pass

if __name__ == '__main__':
    import sys
    db   = sys.argv[1]
    sys.argv = sys.argv[1:]
    fs = NotesFS(db)
    fs.parse(errex=1)
    fs.main()

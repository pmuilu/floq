use crate::error::{Error, Result};
use crate::pipeline::now_millis;
use std::ffi::{c_char, c_int, c_uchar, c_void, CStr, CString};
use std::fmt;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex};

#[allow(non_camel_case_types)]
enum sqlite3 {}
#[allow(non_camel_case_types)]
enum sqlite3_stmt {}

type SqliteDestructor = Option<unsafe extern "C" fn(*mut c_void)>;

const SQLITE_OK: c_int = 0;
const SQLITE_ROW: c_int = 100;
const SQLITE_DONE: c_int = 101;
const SQLITE_OPEN_READWRITE: c_int = 0x0000_0002;
const SQLITE_OPEN_CREATE: c_int = 0x0000_0004;
const SQLITE_OPEN_FULLMUTEX: c_int = 0x0001_0000;

#[link(name = "sqlite3")]
extern "C" {
    fn sqlite3_open_v2(
        filename: *const c_char,
        pp_db: *mut *mut sqlite3,
        flags: c_int,
        z_vfs: *const c_char,
    ) -> c_int;
    fn sqlite3_close_v2(db: *mut sqlite3) -> c_int;
    fn sqlite3_busy_timeout(db: *mut sqlite3, ms: c_int) -> c_int;
    fn sqlite3_exec(
        db: *mut sqlite3,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(*mut c_void, c_int, *mut *mut c_char, *mut *mut c_char) -> c_int,
        >,
        arg: *mut c_void,
        errmsg: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_free(ptr: *mut c_void);
    fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char;
    fn sqlite3_prepare_v2(
        db: *mut sqlite3,
        z_sql: *const c_char,
        n_byte: c_int,
        pp_stmt: *mut *mut sqlite3_stmt,
        pz_tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_bind_text(
        stmt: *mut sqlite3_stmt,
        index: c_int,
        value: *const c_char,
        n_bytes: c_int,
        destructor: SqliteDestructor,
    ) -> c_int;
    fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, index: c_int, value: i64) -> c_int;
    fn sqlite3_step(stmt: *mut sqlite3_stmt) -> c_int;
    fn sqlite3_column_text(stmt: *mut sqlite3_stmt, i_col: c_int) -> *const c_uchar;
    fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, i_col: c_int) -> c_int;
    fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> c_int;
}

#[inline]
fn sqlite_transient() -> SqliteDestructor {
    unsafe { std::mem::transmute::<isize, SqliteDestructor>(-1) }
}

struct SqliteConnection {
    db: *mut sqlite3,
}

// Safety: `sqlite3_open_v2` is called with `SQLITE_OPEN_FULLMUTEX` and all accesses
// are additionally serialized through `Arc<Mutex<SqliteConnection>>`.
unsafe impl Send for SqliteConnection {}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                sqlite3_close_v2(self.db);
            }
            self.db = ptr::null_mut();
        }
    }
}

impl SqliteConnection {
    fn open(path_cstr: &CStr) -> Result<Self> {
        let mut db: *mut sqlite3 = ptr::null_mut();
        let flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
        let rc = unsafe { sqlite3_open_v2(path_cstr.as_ptr(), &mut db, flags, ptr::null()) };
        if rc != SQLITE_OK || db.is_null() {
            let msg = if !db.is_null() {
                let err = Self::last_error_raw(db);
                unsafe { sqlite3_close_v2(db) };
                err
            } else {
                format!("sqlite3_open_v2 failed with code {rc}")
            };
            return Err(Error::Checkpoint(msg));
        }

        unsafe {
            sqlite3_busy_timeout(db, 5_000);
        }

        let conn = Self { db };
        conn.exec_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             CREATE TABLE IF NOT EXISTS floq_checkpoints (
                 source_key TEXT PRIMARY KEY,
                 cursor_value TEXT NOT NULL,
                 updated_at_ms INTEGER NOT NULL
             );",
        )?;
        Ok(conn)
    }

    fn last_error(&self) -> String {
        Self::last_error_raw(self.db)
    }

    fn last_error_raw(db: *mut sqlite3) -> String {
        if db.is_null() {
            return "null sqlite3 handle".to_string();
        }
        unsafe {
            let ptr = sqlite3_errmsg(db);
            if ptr.is_null() {
                "unknown sqlite3 error".to_string()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        }
    }

    fn exec_batch(&self, sql: &str) -> Result<()> {
        let c_sql =
            CString::new(sql).map_err(|e| Error::Checkpoint(format!("invalid SQL string: {e}")))?;
        let mut errmsg: *mut c_char = ptr::null_mut();
        let rc = unsafe {
            sqlite3_exec(
                self.db,
                c_sql.as_ptr(),
                None,
                ptr::null_mut(),
                &mut errmsg,
            )
        };
        if rc != SQLITE_OK {
            let msg = if !errmsg.is_null() {
                let s = unsafe { CStr::from_ptr(errmsg).to_string_lossy().into_owned() };
                unsafe { sqlite3_free(errmsg.cast()) };
                s
            } else {
                self.last_error()
            };
            return Err(Error::Checkpoint(msg));
        }
        Ok(())
    }
}

struct StatementGuard(*mut sqlite3_stmt);

impl Drop for StatementGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                sqlite3_finalize(self.0);
            }
        }
    }
}

/// Persistent SQLite-backed checkpoint store for stream source cursors and offsets.
///
/// Stores `(source_key, cursor_value, updated_at_ms)` rows in a WAL-mode SQLite database
/// (`floq_checkpoints` table) so sources like `BlueskyFirehoseSource` and `FileSource` can
/// resume from their exact position after a process restart.
#[derive(Clone)]
pub struct SqliteCheckpointStore {
    path: PathBuf,
    conn: Arc<Mutex<SqliteConnection>>,
}

impl fmt::Debug for SqliteCheckpointStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteCheckpointStore")
            .field("path", &self.path)
            .finish()
    }
}

impl SqliteCheckpointStore {
    /// Opens or creates a SQLite checkpoint database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        if let Some(parent) = path_buf.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let path_str = path_buf.to_string_lossy();
        let c_path = CString::new(path_str.as_bytes())
            .map_err(|e| Error::Checkpoint(format!("invalid database path: {e}")))?;
        let conn = SqliteConnection::open(&c_path)?;
        Ok(Self {
            path: path_buf,
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Creates an ephemeral in-memory SQLite checkpoint store (useful for testing).
    pub fn in_memory() -> Result<Self> {
        let c_path = CString::new(":memory:").expect("static CString");
        let conn = SqliteConnection::open(&c_path)?;
        Ok(Self {
            path: PathBuf::from(":memory:"),
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Returns the filesystem path (or `:memory:`) of the checkpoint database.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Reads the stored string cursor for `source_key`, returning `Ok(None)` if no checkpoint exists.
    pub fn get_str(&self, source_key: &str) -> Result<Option<String>> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Checkpoint("checkpoint mutex poisoned".to_string()))?;

        const SQL: &[u8] =
            b"SELECT cursor_value FROM floq_checkpoints WHERE source_key = ?1\0";
        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                guard.db,
                SQL.as_ptr().cast(),
                (SQL.len() - 1) as c_int,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(Error::Checkpoint(guard.last_error()));
        }
        let stmt_guard = StatementGuard(stmt);

        let rc = unsafe {
            sqlite3_bind_text(
                stmt_guard.0,
                1,
                source_key.as_ptr().cast(),
                source_key.len() as c_int,
                sqlite_transient(),
            )
        };
        if rc != SQLITE_OK {
            return Err(Error::Checkpoint(guard.last_error()));
        }

        let step_rc = unsafe { sqlite3_step(stmt_guard.0) };
        match step_rc {
            SQLITE_ROW => {
                let text_ptr = unsafe { sqlite3_column_text(stmt_guard.0, 0) };
                let len = unsafe { sqlite3_column_bytes(stmt_guard.0, 0) } as usize;
                if text_ptr.is_null() {
                    Ok(None)
                } else {
                    let bytes = unsafe { std::slice::from_raw_parts(text_ptr, len) };
                    Ok(Some(String::from_utf8_lossy(bytes).into_owned()))
                }
            }
            SQLITE_DONE => Ok(None),
            _ => Err(Error::Checkpoint(guard.last_error())),
        }
    }

    /// Atomically upserts `cursor_value` for `source_key`.
    pub fn set_str(&self, source_key: &str, cursor_value: &str) -> Result<()> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Checkpoint("checkpoint mutex poisoned".to_string()))?;

        const SQL: &[u8] = b"INSERT INTO floq_checkpoints (source_key, cursor_value, updated_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(source_key) DO UPDATE SET
                cursor_value = excluded.cursor_value,
                updated_at_ms = excluded.updated_at_ms\0";

        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                guard.db,
                SQL.as_ptr().cast(),
                (SQL.len() - 1) as c_int,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(Error::Checkpoint(guard.last_error()));
        }
        let stmt_guard = StatementGuard(stmt);

        let now = now_millis() as i64;
        unsafe {
            if sqlite3_bind_text(
                stmt_guard.0,
                1,
                source_key.as_ptr().cast(),
                source_key.len() as c_int,
                sqlite_transient(),
            ) != SQLITE_OK
                || sqlite3_bind_text(
                    stmt_guard.0,
                    2,
                    cursor_value.as_ptr().cast(),
                    cursor_value.len() as c_int,
                    sqlite_transient(),
                ) != SQLITE_OK
                || sqlite3_bind_int64(stmt_guard.0, 3, now) != SQLITE_OK
            {
                return Err(Error::Checkpoint(guard.last_error()));
            }
        }

        let step_rc = unsafe { sqlite3_step(stmt_guard.0) };
        if step_rc != SQLITE_DONE {
            return Err(Error::Checkpoint(guard.last_error()));
        }
        Ok(())
    }

    /// Reads an `i64` cursor (such as a Bluesky ATProto sequence number) for `source_key`.
    pub fn get_i64(&self, source_key: &str) -> Result<Option<i64>> {
        match self.get_str(source_key)? {
            Some(raw) => raw
                .parse::<i64>()
                .map(Some)
                .map_err(|e| Error::Checkpoint(format!("invalid i64 checkpoint '{raw}': {e}"))),
            None => Ok(None),
        }
    }

    /// Persists an `i64` cursor for `source_key`.
    pub fn set_i64(&self, source_key: &str, value: i64) -> Result<()> {
        self.set_str(source_key, &value.to_string())
    }

    /// Reads a `u64` offset (such as a file byte offset) for `source_key`.
    pub fn get_u64(&self, source_key: &str) -> Result<Option<u64>> {
        match self.get_str(source_key)? {
            Some(raw) => raw
                .parse::<u64>()
                .map(Some)
                .map_err(|e| Error::Checkpoint(format!("invalid u64 checkpoint '{raw}': {e}"))),
            None => Ok(None),
        }
    }

    /// Persists a `u64` offset for `source_key`.
    pub fn set_u64(&self, source_key: &str, value: u64) -> Result<()> {
        self.set_str(source_key, &value.to_string())
    }

    /// Deletes the checkpoint entry for `source_key`.
    pub fn delete(&self, source_key: &str) -> Result<()> {
        let guard = self
            .conn
            .lock()
            .map_err(|_| Error::Checkpoint("checkpoint mutex poisoned".to_string()))?;

        const SQL: &[u8] = b"DELETE FROM floq_checkpoints WHERE source_key = ?1\0";
        let mut stmt: *mut sqlite3_stmt = ptr::null_mut();
        let rc = unsafe {
            sqlite3_prepare_v2(
                guard.db,
                SQL.as_ptr().cast(),
                (SQL.len() - 1) as c_int,
                &mut stmt,
                ptr::null_mut(),
            )
        };
        if rc != SQLITE_OK {
            return Err(Error::Checkpoint(guard.last_error()));
        }
        let stmt_guard = StatementGuard(stmt);

        unsafe {
            if sqlite3_bind_text(
                stmt_guard.0,
                1,
                source_key.as_ptr().cast(),
                source_key.len() as c_int,
                sqlite_transient(),
            ) != SQLITE_OK
            {
                return Err(Error::Checkpoint(guard.last_error()));
            }
        }

        let step_rc = unsafe { sqlite3_step(stmt_guard.0) };
        if step_rc != SQLITE_DONE {
            return Err(Error::Checkpoint(guard.last_error()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_checkpoint_crud() {
        let store = SqliteCheckpointStore::in_memory().unwrap();
        assert_eq!(store.get_i64("bluesky:default").unwrap(), None);

        store.set_i64("bluesky:default", 42_000_123).unwrap();
        assert_eq!(store.get_i64("bluesky:default").unwrap(), Some(42_000_123));

        store.set_i64("bluesky:default", 42_000_999).unwrap();
        assert_eq!(store.get_i64("bluesky:default").unwrap(), Some(42_000_999));

        store.delete("bluesky:default").unwrap();
        assert_eq!(store.get_i64("bluesky:default").unwrap(), None);
    }

    #[test]
    fn test_file_persistence_across_reopen() {
        let dir = std::env::temp_dir().join(format!(
            "floq_checkpoint_test_{}_{}",
            std::process::id(),
            now_millis()
        ));
        let db_path = dir.join("checkpoints.sqlite");

        {
            let store = SqliteCheckpointStore::open(&db_path).unwrap();
            store.set_u64("file:/var/log/events.log", 8192).unwrap();
            store.set_str("custom:cursor", "cursor_abc_123").unwrap();
        }

        {
            let reopened = SqliteCheckpointStore::open(&db_path).unwrap();
            assert_eq!(
                reopened.get_u64("file:/var/log/events.log").unwrap(),
                Some(8192)
            );
            assert_eq!(
                reopened.get_str("custom:cursor").unwrap().as_deref(),
                Some("cursor_abc_123")
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}

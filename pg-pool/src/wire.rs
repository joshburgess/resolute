//! pg-wired integration: implements [`Poolable`] for [`pg_wired::WireConn`].

use crate::Poolable;

/// Newtype wrapper around [`pg_wired::WireConn`] that implements [`Poolable`].
pub struct WirePoolable(pub pg_wired::WireConn);

impl std::ops::Deref for WirePoolable {
    type Target = pg_wired::WireConn;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for WirePoolable {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Poolable for WirePoolable {
    type Error = pg_wired::PgWireError;

    async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Self::Error> {
        let conn = pg_wired::WireConn::connect(addr, user, password, database).await?;
        Ok(WirePoolable(conn))
    }

    fn has_pending_data(&self) -> bool {
        self.0.has_pending_data()
    }
}

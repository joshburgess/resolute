//! pg-wire integration: implements [`Poolable`] for [`pg_wire::WireConn`].

use crate::Poolable;

/// Newtype wrapper around [`pg_wire::WireConn`] that implements [`Poolable`].
pub struct WirePoolable(pub pg_wire::WireConn);

impl std::ops::Deref for WirePoolable {
    type Target = pg_wire::WireConn;
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
    type Error = pg_wire::PgWireError;

    async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, Self::Error> {
        let conn = pg_wire::WireConn::connect(addr, user, password, database).await?;
        Ok(WirePoolable(conn))
    }

    fn has_pending_data(&self) -> bool {
        self.0.has_pending_data()
    }
}

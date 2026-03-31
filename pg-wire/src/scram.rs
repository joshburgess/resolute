//! SCRAM-SHA-256 authentication for PostgreSQL.
//! Supports both SCRAM-SHA-256 (no channel binding) and SCRAM-SHA-256-PLUS
//! (tls-server-end-point channel binding when TLS is active).

use base64::Engine;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

/// Channel binding mode for SCRAM authentication.
#[derive(Clone)]
#[allow(dead_code)]
pub enum ChannelBinding {
    /// No channel binding (SCRAM-SHA-256). GS2 header: `n,,`
    None,
    /// tls-server-end-point channel binding (SCRAM-SHA-256-PLUS).
    /// Contains the SHA-256 hash of the server's TLS certificate.
    TlsServerEndPoint(Vec<u8>),
}

/// SCRAM client state machine.
pub struct ScramClient {
    password: String,
    nonce: String,
    client_first_bare: String,
    channel_binding: ChannelBinding,
}

impl ScramClient {
    /// Create a new SCRAM client and generate the client-first-message.
    /// `channel_binding` should be `None` for plain connections or
    /// `TlsServerEndPoint(cert_hash)` for TLS connections.
    pub fn new(password: &str, channel_binding: ChannelBinding) -> (Self, Vec<u8>) {
        let nonce = generate_nonce();
        let client_first_bare = format!("n=,r={nonce}");

        // GS2 header: "p=tls-server-end-point,," for channel binding, "n,," otherwise.
        let gs2_header = match &channel_binding {
            ChannelBinding::None => "n,,".to_string(),
            ChannelBinding::TlsServerEndPoint(_) => "p=tls-server-end-point,,".to_string(),
        };
        let client_first_msg = format!("{gs2_header}{client_first_bare}");

        (
            ScramClient {
                password: password.to_string(),
                nonce,
                client_first_bare,
                channel_binding,
            },
            client_first_msg.into_bytes(),
        )
    }

    /// Process the server-first-message and produce the client-final-message.
    pub fn process_server_first(&self, server_first: &[u8]) -> Result<Vec<u8>, String> {
        let server_first = std::str::from_utf8(server_first)
            .map_err(|e| format!("Invalid UTF-8 in server-first: {e}"))?;

        let mut server_nonce = "";
        let mut salt_b64 = "";
        let mut iterations = 0u32;

        for part in server_first.split(',') {
            if let Some(v) = part.strip_prefix("r=") {
                server_nonce = v;
            } else if let Some(v) = part.strip_prefix("s=") {
                salt_b64 = v;
            } else if let Some(v) = part.strip_prefix("i=") {
                iterations = v.parse().map_err(|_| "Bad iteration count")?;
            }
        }

        if !server_nonce.starts_with(&self.nonce) {
            return Err("Server nonce doesn't start with client nonce".into());
        }

        let salt = B64
            .decode(salt_b64)
            .map_err(|e| format!("Bad salt base64: {e}"))?;

        let salted_password = hi(&self.password, &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);

        // Channel binding data for client-final-message.
        let channel_binding_data = match &self.channel_binding {
            ChannelBinding::None => B64.encode(b"n,,"),
            ChannelBinding::TlsServerEndPoint(cert_hash) => {
                let mut cb = b"p=tls-server-end-point,,".to_vec();
                cb.extend_from_slice(cert_hash);
                B64.encode(&cb)
            }
        };

        let client_final_without_proof = format!("c={channel_binding_data},r={server_nonce}");
        let auth_message = format!(
            "{},{server_first},{client_final_without_proof}",
            self.client_first_bare
        );

        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();

        let proof_b64 = B64.encode(&proof);
        let client_final = format!("{client_final_without_proof},p={proof_b64}");

        Ok(client_final.into_bytes())
    }
}

/// PBKDF2 with HMAC-SHA-256 (Hi function from RFC 5802).
fn hi(password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut mac =
        HmacSha256::new_from_slice(password.as_bytes()).expect("HMAC accepts any key size");
    mac.update(salt);
    mac.update(&1u32.to_be_bytes());
    let mut u = mac.finalize().into_bytes().to_vec();
    let mut result = u.clone();

    for _ in 1..iterations {
        let mut mac =
            HmacSha256::new_from_slice(password.as_bytes()).expect("HMAC accepts any key size");
        mac.update(&u);
        u = mac.finalize().into_bytes().to_vec();
        for (r, x) in result.iter_mut().zip(u.iter()) {
            *r ^= x;
        }
    }

    result
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256(data: &[u8]) -> Vec<u8> {
    Sha256::digest(data).to_vec()
}

fn generate_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..24).map(|_| rng.random::<u8>()).collect();
    B64.encode(&bytes)
}

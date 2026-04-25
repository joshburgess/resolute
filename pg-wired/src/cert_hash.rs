//! `tls-server-end-point` certificate hashing for SCRAM-SHA-256-PLUS channel
//! binding.
//!
//! RFC 5929 §4.1 specifies the channel binding hash as the digest of the
//! server's TLS certificate using the certificate's signature hash algorithm.
//! SHA-1 and MD5 are forbidden and must be upgraded to SHA-256. PostgreSQL's
//! backend (`be_tls_get_certificate_hash` in `be-secure-openssl.c`) follows
//! the same rule, so a client that always hashes with SHA-256 will fail
//! channel binding against a server cert signed with SHA-384, SHA-512, or
//! Ed25519.

use sha2::Digest;

/// Hash a DER-encoded server certificate using the algorithm derived from
/// its `signatureAlgorithm` OID.
///
/// SHA-1 and MD5 collapse to SHA-256 (per RFC 5929). RSASSA-PSS and unknown
/// algorithms also fall back to SHA-256, matching the most common deployment
/// case; if PostgreSQL disagrees the SCRAM exchange will fail with a
/// `channel-binding-mismatch` error and the caller can switch to a non-PSS
/// cert.
pub(crate) fn cert_signature_hash(cert_der: &[u8]) -> Vec<u8> {
    match parse_signature_oid(cert_der).and_then(|oid| sig_digest_for_oid(&oid)) {
        Some(SigDigest::Sha384) => sha2::Sha384::digest(cert_der).to_vec(),
        Some(SigDigest::Sha512) => sha2::Sha512::digest(cert_der).to_vec(),
        // SHA-256 (preferred), SHA-1/MD5 (upgraded), unknown (best-effort).
        _ => sha2::Sha256::digest(cert_der).to_vec(),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SigDigest {
    Sha256,
    Sha384,
    Sha512,
}

fn sig_digest_for_oid(oid: &[u32]) -> Option<SigDigest> {
    match oid {
        // PKCS#1 v1.5 RSA: sha{256,384,512}WithRSAEncryption
        [1, 2, 840, 113549, 1, 1, 11] => Some(SigDigest::Sha256),
        [1, 2, 840, 113549, 1, 1, 12] => Some(SigDigest::Sha384),
        [1, 2, 840, 113549, 1, 1, 13] => Some(SigDigest::Sha512),
        // ANSI X9.62 ECDSA: ecdsa-with-SHA{256,384,512}
        [1, 2, 840, 10045, 4, 3, 2] => Some(SigDigest::Sha256),
        [1, 2, 840, 10045, 4, 3, 3] => Some(SigDigest::Sha384),
        [1, 2, 840, 10045, 4, 3, 4] => Some(SigDigest::Sha512),
        // Ed25519 internally hashes with SHA-512.
        [1, 3, 101, 112] => Some(SigDigest::Sha512),
        _ => None,
    }
}

/// DER-parse a `Certificate` and return its `signatureAlgorithm.algorithm` OID.
///
/// ```text
/// Certificate ::= SEQUENCE {
///     tbsCertificate          TBSCertificate,        -- skipped
///     signatureAlgorithm      AlgorithmIdentifier,
///     signatureValue          BIT STRING
/// }
/// AlgorithmIdentifier ::= SEQUENCE {
///     algorithm   OBJECT IDENTIFIER,
///     parameters  ANY DEFINED BY algorithm OPTIONAL
/// }
/// ```
fn parse_signature_oid(der: &[u8]) -> Option<Vec<u32>> {
    let (_, outer_body) = read_tlv(der, 0x30)?;
    let (tbs_total_len, _) = read_tlv(outer_body, 0x30)?;
    let after_tbs = outer_body.get(tbs_total_len..)?;
    let (_, alg_body) = read_tlv(after_tbs, 0x30)?;
    let (_, oid_body) = read_tlv(alg_body, 0x06)?;
    decode_oid(oid_body)
}

/// Read a DER TLV with the expected tag.
/// Returns `(total_len_including_header, content_slice)`.
fn read_tlv(buf: &[u8], expected_tag: u8) -> Option<(usize, &[u8])> {
    if buf.first()? != &expected_tag {
        return None;
    }
    let (len_bytes, content_len) = read_length(buf.get(1..)?)?;
    let header_len = 1 + len_bytes;
    let total_len = header_len.checked_add(content_len)?;
    let content = buf.get(header_len..total_len)?;
    Some((total_len, content))
}

/// Decode a DER length octet sequence. Returns `(bytes_consumed, length_value)`.
fn read_length(buf: &[u8]) -> Option<(usize, usize)> {
    let first = *buf.first()?;
    if first & 0x80 == 0 {
        return Some((1, first as usize));
    }
    let n = (first & 0x7f) as usize;
    if n == 0 || n > std::mem::size_of::<usize>() {
        return None;
    }
    let bytes = buf.get(1..1 + n)?;
    let mut len = 0usize;
    for &b in bytes {
        len = (len << 8) | b as usize;
    }
    Some((1 + n, len))
}

fn decode_oid(buf: &[u8]) -> Option<Vec<u32>> {
    if buf.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(8);
    let first = buf[0] as u32;
    out.push(first / 40);
    out.push(first % 40);
    let mut acc: u32 = 0;
    for &b in &buf[1..] {
        acc = acc.checked_shl(7)?.checked_add((b & 0x7f) as u32)?;
        if b & 0x80 == 0 {
            out.push(acc);
            acc = 0;
        }
    }
    if acc != 0 {
        // Trailing high bit set: truncated OID encoding.
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHA256_RSA: &[u8] = include_bytes!("../tests/fixtures/certs/sha256_rsa.der");
    const SHA384_RSA: &[u8] = include_bytes!("../tests/fixtures/certs/sha384_rsa.der");
    const SHA512_RSA: &[u8] = include_bytes!("../tests/fixtures/certs/sha512_rsa.der");
    const ECDSA_P256: &[u8] = include_bytes!("../tests/fixtures/certs/ecdsa_p256.der");

    fn hex(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            use std::fmt::Write;
            write!(s, "{:02x}", b).unwrap();
        }
        s
    }

    #[test]
    fn parses_sha256_rsa_oid() {
        let oid = parse_signature_oid(SHA256_RSA).expect("parse OID");
        assert_eq!(oid, vec![1, 2, 840, 113549, 1, 1, 11]);
    }

    #[test]
    fn parses_sha384_rsa_oid() {
        let oid = parse_signature_oid(SHA384_RSA).expect("parse OID");
        assert_eq!(oid, vec![1, 2, 840, 113549, 1, 1, 12]);
    }

    #[test]
    fn parses_sha512_rsa_oid() {
        let oid = parse_signature_oid(SHA512_RSA).expect("parse OID");
        assert_eq!(oid, vec![1, 2, 840, 113549, 1, 1, 13]);
    }

    #[test]
    fn parses_ecdsa_p256_oid() {
        let oid = parse_signature_oid(ECDSA_P256).expect("parse OID");
        assert_eq!(oid, vec![1, 2, 840, 10045, 4, 3, 2]);
    }

    #[test]
    fn sha256_rsa_cert_hashes_with_sha256() {
        // Matches: openssl dgst -sha256 sha256_rsa.der
        assert_eq!(
            hex(&cert_signature_hash(SHA256_RSA)),
            "bc448a6f75ffa78d35bf564c84a6e64d4cd34041af02910d5bfb39336e8e03d6"
        );
    }

    #[test]
    fn sha384_rsa_cert_hashes_with_sha384() {
        // Matches: openssl dgst -sha384 sha384_rsa.der
        assert_eq!(
            hex(&cert_signature_hash(SHA384_RSA)),
            "cd86e4a9ae3c78638923748047b49680a4034e24deb575dc04ca8b7963513ce0\
             4af2a8f0c02092109fb8aa3eb6cb1428"
        );
    }

    #[test]
    fn sha512_rsa_cert_hashes_with_sha512() {
        // Matches: openssl dgst -sha512 sha512_rsa.der
        assert_eq!(
            hex(&cert_signature_hash(SHA512_RSA)),
            "680d4350f0338b6dfe07329fcea1256cd8e25fe985cf4024707daaa26046987498e3ae39f78a162650ebd5ce71c6127eb833321adb3de6259a190badfc0c45e5"
        );
    }

    #[test]
    fn ecdsa_p256_cert_hashes_with_sha256() {
        // ecdsa-with-SHA256 → SHA-256.
        assert_eq!(
            hex(&cert_signature_hash(ECDSA_P256)),
            "b3c57ba31f44823d2ad7c02a7581f4a033cac90a32fc6a7b8bcce3354b2b472b"
        );
    }

    #[test]
    fn unknown_oid_falls_back_to_sha256() {
        // Made-up OID (not in our table) should hash with SHA-256.
        assert_eq!(sig_digest_for_oid(&[1, 2, 3, 4, 5]), None);
    }

    #[test]
    fn truncated_der_returns_none() {
        assert!(parse_signature_oid(&SHA256_RSA[..40]).is_none());
        assert!(parse_signature_oid(&[]).is_none());
        assert!(parse_signature_oid(&[0x30]).is_none());
    }

    #[test]
    fn decodes_known_oid_encodings() {
        // 1.2.840.113549.1.1.11 (sha256WithRSAEncryption)
        // DER: 2A 86 48 86 F7 0D 01 01 0B
        let bytes = [0x2A, 0x86, 0x48, 0x86, 0xF7, 0x0D, 0x01, 0x01, 0x0B];
        assert_eq!(
            decode_oid(&bytes).unwrap(),
            vec![1, 2, 840, 113549, 1, 1, 11]
        );
    }
}

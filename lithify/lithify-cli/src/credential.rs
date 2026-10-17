use std::convert::Infallible;
use std::fmt::{self, Debug, Formatter};
use std::str::FromStr;

#[derive(Clone)]
pub struct Credential {
    key_id: String,
    secret: Secret,
}

impl Credential {
    pub fn new(key_id: String, secret: Secret) -> Self {
        Self { key_id, secret }
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    pub fn secret(&self) -> &str {
        self.secret.read()
    }
}

#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    pub fn read(&self) -> &str {
        &self.0
    }
}

impl Debug for Secret {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("***")
    }
}

impl FromStr for Secret {
    type Err = Infallible;

    fn from_str(secret: &str) -> Result<Self, Self::Err> {
        Ok(Self(secret.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_secret_is_not_printed_where_it_is_read() {
        let secret: Secret = "rustfsadmin".parse().unwrap();

        assert_eq!(format!("{secret:?}"), "***");
        assert_eq!(
            Credential::new("key".to_owned(), secret).secret(),
            "rustfsadmin"
        );
    }
}

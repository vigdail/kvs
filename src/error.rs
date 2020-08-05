use failure::Fail;

#[derive(Fail, Debug)]
pub enum Error {
    #[fail(display = "error")]
    UnableToSet,
    #[fail(display = "no key found: {}", _0)]
    KeyDoesNotExist(String),
}

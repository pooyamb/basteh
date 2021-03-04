#![allow(unused_variables, unused_imports)]
use serde::{de::DeserializeOwned, Serialize};

use crate::error::{Result, StorageError};

/// An enum representing the format used for serde interactions
///
/// It is default to None if no format is specified and none of the serde
/// extension features are activated which will cause run time error if
/// used.
///
/// requires "with-serde" feature
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Format {
    #[cfg(feature = "serde-json")]
    Json,
    #[cfg(feature = "serde-cbor")]
    Cbor,
    #[cfg(feature = "serde-ron")]
    Ron,
    #[cfg(feature = "serde-yaml")]
    Yaml,
    #[cfg(feature = "serde-bincode")]
    Bincode,
    #[cfg(feature = "serde-xml")]
    Xml,
    #[cfg(not(any(
        feature = "serde-json",
        feature = "serde-cbor",
        feature = "serde-ron",
        feature = "serde-yaml",
        feature = "serde-bincode",
        feature = "serde-xml"
    )))]
    None,
}

impl Default for Format {
    #[allow(unreachable_code)]
    fn default() -> Self {
        #[cfg(feature = "serde-json")]
        return Format::Json;
        #[cfg(feature = "serde-cbor")]
        return Format::Cbor;
        #[cfg(feature = "serde-ron")]
        return Format::Ron;
        #[cfg(feature = "serde-yaml")]
        return Format::Yaml;
        #[cfg(feature = "serde-bincode")]
        return Format::Bincode;
        #[cfg(feature = "serde-xml")]
        return Format::Xml;
        #[cfg(not(any(
            feature = "serde-json",
            feature = "serde-cbor",
            feature = "serde-ron",
            feature = "serde-yaml",
            feature = "serde-bincode",
            feature = "serde-xml"
        )))]
        Format::None
    }
}

/// Serializes a generic value based on the format specified
///
/// ## Errors
/// It will result in error if serialization fails or if there is no serde
/// extension feature is activated.
pub fn serialize<T>(value: &T, format: &Format) -> Result<Vec<u8>>
where
    T: Serialize,
{
    match format {
        #[cfg(feature = "serde-json")]
        Format::Json => serde_json::to_vec(value).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-cbor")]
        Format::Cbor => serde_cbor::to_vec(value).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-ron")]
        Format::Ron => {
            let mut writer = Vec::new();
            ron::ser::to_writer(&mut writer, value)
                .map_err(|_| StorageError::SerializationError)?;
            Ok(writer)
        }
        #[cfg(feature = "serde-yaml")]
        Format::Yaml => serde_yaml::to_vec(value).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-bincode")]
        Format::Bincode => bincode::serialize(value).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-xml")]
        Format::Xml => {
            let mut writer = Vec::new();
            quick_xml::se::to_writer(&mut writer, value)
                .map_err(|_| StorageError::SerializationError)?;
            Ok(writer)
        }
        #[cfg(not(any(
            feature = "serde-json",
            feature = "serde-cbor",
            feature = "serde-ron",
            feature = "serde-yaml",
            feature = "serde-bincode",
            feature = "serde-xml"
        )))]
        Format::None => {
            panic!("At least one of the serde extension features should be active to use serialize")
        }
    }
}

/// Deserializes a generic value based on the format specified
///
/// ## Errors
/// It will result in error if deserialization fails or if there is no serde
/// extension feature is activated.
pub fn deserialize<T>(slice: &[u8], format: &Format) -> Result<T>
where
    T: DeserializeOwned,
{
    match format {
        #[cfg(feature = "serde-json")]
        Format::Json => serde_json::from_slice(slice).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-cbor")]
        Format::Cbor => serde_cbor::from_slice(slice).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-ron")]
        Format::Ron => ron::de::from_bytes(slice).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-yaml")]
        Format::Yaml => serde_yaml::from_slice(slice).map_err(|_| StorageError::SerializationError),
        #[cfg(feature = "serde-bincode")]
        Format::Bincode => {
            bincode::deserialize(slice).map_err(|_| StorageError::SerializationError)
        }
        #[cfg(feature = "serde-xml")]
        Format::Xml => {
            quick_xml::de::from_reader(slice).map_err(|_| StorageError::SerializationError)
        }
        #[cfg(not(any(
            feature = "serde-json",
            feature = "serde-cbor",
            feature = "serde-ron",
            feature = "serde-yaml",
            feature = "serde-bincode",
            feature = "serde-xml"
        )))]
        Format::None => panic!(
            "At least one of the serde extension features should be active to use deserialzie"
        ),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Eq, PartialEq)]
    struct Human {
        name: String,
        height: u32,
        says_hello: bool,
    }

    fn get_mamad() -> Human {
        Human {
            name: "Mamad".to_string(),
            height: 160,
            says_hello: false,
        }
    }

    #[cfg(not(any(
        feature = "serde-json",
        feature = "serde-cbor",
        feature = "serde-ron",
        feature = "serde-yaml",
        feature = "serde-bincode",
        feature = "serde-xml"
    )))]
    mod noext_test {
        use super::*;

        #[test]
        fn test_features() {
            let format = Format::default();
            assert!(format == Format::None);
        }

        #[test]
        #[should_panic(
            expected = "At least one of the serde extension features should be active to use deserialzie"
        )]
        fn test_deser_panic() {
            let format = Format::default();
            let _: Result<String> = deserialize("panic".as_bytes(), &format);
        }

        #[test]
        #[should_panic(
            expected = "At least one of the serde extension features should be active to use serialize"
        )]
        fn test_ser_panic() {
            let format = Format::default();
            let code = "panic".as_bytes().to_vec();
            let _: Result<Vec<u8>> = serialize(&code, &format);
        }
    }

    #[cfg(any(
        feature = "serde-json",
        feature = "serde-cbor",
        feature = "serde-ron",
        feature = "serde-yaml",
        feature = "serde-bincode",
        feature = "serde-xml"
    ))]
    #[test]
    fn test_serde() {
        let format = Format::default();
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        let demamad: Human = deserialize(&s, &format).unwrap();

        assert!(mamad == demamad)
    }

    #[cfg(any(
        feature = "serde-json",
        feature = "serde-cbor",
        feature = "serde-ron",
        feature = "serde-yaml",
        feature = "serde-bincode",
        feature = "serde-xml"
    ))]
    #[test]
    fn test_wrong_serde() {
        let format = Format::default();
        let demamad: Result<Human> = deserialize(b"Some random bytes", &format);

        assert!(demamad.is_err())
    }

    #[cfg(any(feature = "serde-json"))]
    #[test]
    fn test_json() {
        let format = Format::Json;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(s == br#"{"name":"Mamad","height":160,"says_hello":false}"#);
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }

    #[cfg(any(feature = "serde-cbor"))]
    #[test]
    fn test_cbor() {
        let format = Format::Cbor;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(
            s == [
                163, 100, 110, 97, 109, 101, 101, 77, 97, 109, 97, 100, 102, 104, 101, 105, 103,
                104, 116, 24, 160, 106, 115, 97, 121, 115, 95, 104, 101, 108, 108, 111, 244
            ]
        );
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }

    #[cfg(any(feature = "serde-ron"))]
    #[test]
    fn test_ron() {
        let format = Format::Ron;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(s == br#"(name:"Mamad",height:160,says_hello:false)"#);
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }

    #[cfg(any(feature = "serde-yaml"))]
    #[test]
    fn test_yaml() {
        let format = Format::Yaml;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(s == "---\nname: Mamad\nheight: 160\nsays_hello: false".as_bytes());
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }

    #[cfg(any(feature = "serde-bincode"))]
    #[test]
    fn test_bincode() {
        let format = Format::Bincode;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(s == [5, 0, 0, 0, 0, 0, 0, 0, 77, 97, 109, 97, 100, 160, 0, 0, 0, 0]);
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }

    #[cfg(any(feature = "serde-xml"))]
    #[test]
    fn test_xml() {
        let format = Format::Xml;
        let mamad = get_mamad();
        let s = serialize(&mamad, &format).unwrap();
        assert!(s == br#"<Human name="Mamad" height="160" says_hello="false"/>"#);
        let demamad: Human = deserialize(&s, &format).unwrap();
        assert!(mamad == demamad)
    }
}

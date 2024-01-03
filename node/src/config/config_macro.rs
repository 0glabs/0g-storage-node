// Copyright 2019 Conflux Foundation. All rights reserved.
// Conflux is free software and distributed under GNU General Public License.
// See http://www.gnu.org/licenses/

macro_rules! if_option {
	(Option<$type:ty>, THEN {$($then:tt)*} ELSE {$($otherwise:tt)*}) => (
		$($then)*
	);
	($type:ty, THEN {$($then:tt)*} ELSE {$($otherwise:tt)*}) => (
		$($otherwise)*
	);
}

macro_rules! if_not_vector {
	(Vec<$type:ty>, THEN {$($then:tt)*}) => (
		{}
	);
	($type:ty, THEN {$($then:tt)*}) => (
        $($then)*
	);
}

macro_rules! underscore_to_hyphen {
    ($e:expr) => {
        str::replace($e, "_", "-")
    };
}

macro_rules! build_config{
    ($(($name:ident, ($($type:tt)+), $default:expr))*) => {
        #[derive(Debug, PartialEq, Eq, Clone, serde::Deserialize)]
        pub struct RawConfiguration {
            $(pub $name: $($type)+,)*
        }

        impl Default for RawConfiguration {
            fn default() -> Self {
                RawConfiguration {
                    $($name: $default,)*
                }
            }
        }

        impl RawConfiguration {
            // First parse arguments from config file, and then parse them from command line.
            // Replace the ones from config file with the ones from commandline for duplicates.
            pub fn parse(matches: &clap::ArgMatches) -> Result<RawConfiguration, String> {
                let mut config = RawConfiguration::default();

                // read from config file
                if let Some(config_file) = matches.value_of("config") {
                    let config_value = std::fs::read_to_string(config_file)
                        .map_err(|e| format!("failed to read configuration file: {:?}", e))?
                        .parse::<toml::Value>()
                        .map_err(|e| format!("failed to parse configuration file: {:?}", e))?;

                    $(
                        if let Some(value) = config_value.get(stringify!($name)) {
                            config.$name = if_option!($($type)+,
                                THEN { Some(value.clone().try_into().map_err(|e| format!("Invalid {}: err={:?}", stringify!($name), e).to_owned())?) }
                                ELSE { value.clone().try_into().map_err(|e| format!("Invalid {}: err={:?}", stringify!($name), e).to_owned())? }
                            );
                        }
                    )*
                }

                // read from command line
                $(
                    #[allow(unused_variables)]
                    if let Some(value) = matches.value_of(underscore_to_hyphen!(stringify!($name))) {
                        if_not_vector!($($type)+, THEN {
                            config.$name = if_option!($($type)+,
                                THEN{ Some(value.parse().map_err(|_| concat!("Invalid ", stringify!($name)).to_owned())?) }
                                ELSE{ value.parse().map_err(|_| concat!("Invalid ", stringify!($name)).to_owned())? }
                            )}
                        )
                    }
                )*

                Ok(config)
            }
        }
    }
}

pub(crate) use {build_config, if_not_vector, if_option, underscore_to_hyphen};

use std::env;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let address = match env::var("OBSERVE_HEALTH_ADDR") {
        Ok(value) => value,
        Err(env::VarError::NotPresent) => "127.0.0.1:8080".to_owned(),
        Err(error) => return Err(error.into()),
    };
    let listener = observe_health::bind_loopback(&address)?;
    eprintln!("process health listening on {}", listener.local_addr()?);
    observe_health::serve(listener)?;
    Ok(())
}

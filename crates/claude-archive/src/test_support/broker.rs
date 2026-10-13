//! A private `nats-server` child for the tests that need a broker of their own: one whose only
//! user is the reviewed Claude stanza, or one whose connection counter the test can read.
//!
//! The shared broker of `CLAUDE_ARCHIVE_TEST_NATS_URL` cannot serve them: it has no authorization
//! and other tests connect to it at the same time. The server binary must be on `PATH` (the CI
//! step `Put nats-server on PATH` copies it out of the pinned container); a missing binary fails
//! the test, it never skips it.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use uuid::Uuid;

/// The reviewed stanza of this service, relative to the crate directory.
const IDENTITY_FRAGMENT: &str = "../../deploy/nats/identity.conf";
const PLACEHOLDER_PREFIX: &str = "UREPLACE_ME_WITH_THE_PUBLIC_NKEY_OF_RATATOSKR_CLAUDE_";

/// Held from picking a port until the server listens on it. Tests of one binary run in parallel,
/// and a port is only taken once its server listens, so without this two brokers pick one port.
static START: Mutex<()> = Mutex::new(());

/// A free port in the range reserved for this repository's private test servers.
fn free_port() -> Result<u16, Box<dyn std::error::Error>> {
    (57_820..=57_839)
        .find(|port| TcpListener::bind(("127.0.0.1", *port)).is_ok())
        .ok_or_else(|| "no free port in 57820..=57839".into())
}

/// A `nats-server` child with `JetStream` and a monitoring port, killed when dropped.
#[derive(Debug)]
pub struct Broker {
    child: Option<Child>,
    port: u16,
    monitor_port: u16,
    directory: PathBuf,
    configuration: PathBuf,
    /// The seed of the all-powerful setup user; empty for an open broker.
    pub admin_seed: PathBuf,
    /// The seed of the identity that owns the reviewed Claude stanza; empty for an open broker.
    pub service_seed: PathBuf,
}

impl Drop for Broker {
    fn drop(&mut self) {
        self.stop();
        let _removed = std::fs::remove_dir_all(&self.directory);
    }
}

fn write_seed(
    directory: &Path,
    name: &str,
    pair: &nkeys::KeyPair,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = directory.join(name);
    std::fs::write(&path, pair.seed()?)?;
    Ok(path)
}

/// The reviewed fragment with its placeholder swapped for the test identity's public key.
fn service_stanza(public_key: &str) -> Result<String, Box<dyn std::error::Error>> {
    let fragment =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(IDENTITY_FRAGMENT))?;
    assert!(
        fragment.contains(PLACEHOLDER_PREFIX),
        "the fragment must carry the CLAUDE placeholder nkey"
    );
    Ok(fragment
        .lines()
        .map(|line| {
            if line.trim_start().starts_with("nkey:") {
                format!("nkey: {public_key}")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

impl Broker {
    /// A broker without authorization, for the tests that count connections. `service_seed` still
    /// names a valid seed file, because the pump reads one whatever the server asks for.
    ///
    /// # Errors
    ///
    /// When the server binary is missing or does not start listening in time.
    pub fn open() -> Result<Self, Box<dyn std::error::Error>> {
        let mut broker = Self::start("", None)?;
        broker.service_seed = write_seed(
            &broker.directory,
            "service.nkey",
            &nkeys::KeyPair::new_user(),
        )?;
        Ok(broker)
    }

    /// A broker whose only users are a setup user and the reviewed Claude stanza.
    ///
    /// # Errors
    ///
    /// When the server binary is missing, the identity fragment cannot be read, or the server does not start listening in time.
    pub fn authorized() -> Result<Self, Box<dyn std::error::Error>> {
        let admin = nkeys::KeyPair::new_user();
        let service = nkeys::KeyPair::new_user();
        let users = format!(
            "authorization {{ users: [\n\
             {{ nkey: {admin_key}, permissions: {{ publish: {{ allow: [\">\"] }}, subscribe: {{ allow: [\">\"] }} }} }},\n\
             {stanza}\n] }}\n",
            admin_key = admin.public_key(),
            stanza = service_stanza(&service.public_key())?,
        );
        Self::start(&users, Some(&(admin, service)))
    }

    fn start(
        authorization: &str,
        users: Option<&(nkeys::KeyPair, nkeys::KeyPair)>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!("claude-broker-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&directory)?;
        let (admin_seed, service_seed) = match users {
            Some((admin, service)) => (
                write_seed(&directory, "admin.nkey", admin)?,
                write_seed(&directory, "service.nkey", service)?,
            ),
            None => (PathBuf::new(), PathBuf::new()),
        };
        let _starting = START
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let port = free_port()?;
        let monitor_port = (port + 1..=57_839)
            .find(|candidate| TcpListener::bind(("127.0.0.1", *candidate)).is_ok())
            .ok_or("no free monitoring port in the reserved range")?;
        let configuration = directory.join("broker.conf");
        std::fs::write(
            &configuration,
            format!(
                "port: {port}\nhost: 127.0.0.1\nhttp: 127.0.0.1:{monitor_port}\n\
                 jetstream {{ store_dir: \"{store}\" }}\n{authorization}",
                store = directory.join("store").display(),
            ),
        )?;
        let mut broker = Self {
            child: None,
            port,
            monitor_port,
            directory,
            configuration,
            admin_seed,
            service_seed,
        };
        broker.spawn()?;
        Ok(broker)
    }

    fn spawn(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let child = Command::new("nats-server")
            .arg("-c")
            .arg(&self.configuration)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        self.child = Some(child);
        let deadline = Instant::now() + Duration::from_secs(10);
        while TcpStream::connect(("127.0.0.1", self.port)).is_err()
            || TcpStream::connect(("127.0.0.1", self.monitor_port)).is_err()
        {
            if Instant::now() >= deadline {
                return Err("the private broker did not start listening".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(())
    }

    /// Stops the server; its port stays reserved for a later [`Broker::restart`].
    pub fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _killed = child.kill();
            let _reaped = child.wait();
        }
    }

    /// Starts the stopped server again on the same ports and store.
    ///
    /// # Errors
    ///
    /// When the server does not start listening again in time.
    pub fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stop();
        self.spawn()
    }

    /// Replaces the service seed file with a fresh key the broker does not authorize, as a
    /// rotation that was applied to the file before it reached the broker would.
    ///
    /// # Errors
    ///
    /// When the seed file cannot be written.
    pub fn rotate_service_seed(&self) -> Result<(), Box<dyn std::error::Error>> {
        write_seed(&self.directory, "service.nkey", &nkeys::KeyPair::new_user())?;
        Ok(())
    }

    /// The endpoint services connect to.
    #[must_use]
    pub fn url(&self) -> String {
        format!("nats://127.0.0.1:{}", self.port)
    }

    /// How many client connections the server accepted since it started, read from its
    /// monitoring endpoint.
    ///
    /// # Errors
    ///
    /// When the monitoring endpoint cannot be read or does not report a connection count.
    pub fn total_connections(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(("127.0.0.1", self.monitor_port))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.write_all(b"GET /varz HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n")?;
        let mut response = String::new();
        stream.read_to_string(&mut response)?;
        let body = response
            .split_once("\r\n\r\n")
            .ok_or("the monitoring answer has no body")?
            .1;
        let varz: serde_json::Value = serde_json::from_str(body)?;
        varz.get("total_connections")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| "the monitoring answer has no total_connections".into())
    }

    /// A client of the setup user (or of the open broker), used to provision streams.
    ///
    /// # Errors
    ///
    /// When the seed file cannot be read or the connection is refused.
    pub async fn admin_client(&self) -> Result<async_nats::Client, Box<dyn std::error::Error>> {
        if self.admin_seed.as_os_str().is_empty() {
            return Ok(async_nats::connect(self.url()).await?);
        }
        let seed = std::fs::read_to_string(&self.admin_seed)?;
        Ok(
            async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
                .connect(self.url())
                .await?,
        )
    }

    /// Creates the events stream with a short duplicate window, and the Edge-provisioned durable
    /// `durable` on `filter_subject` (Explicit acknowledgements, 30 s wait, unlimited deliveries),
    /// as Edge does for a producer's fixed consumer.
    ///
    /// The short window lets a test see a second stream copy of a message that is published again
    /// with the same `Nats-Msg-Id`; the 120 s window of the deployed stream would collapse it.
    ///
    /// # Errors
    ///
    /// When the setup user cannot connect or the stream or consumer cannot be created.
    pub async fn provision_durable(
        &self,
        durable: &str,
        filter_subject: &str,
        duplicate_window: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.admin_client().await?;
        let stream = async_nats::jetstream::new(client.clone())
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: "ratatoskr_events".to_owned(),
                subjects: vec!["evt.>".to_owned()],
                duplicate_window,
                ..Default::default()
            })
            .await?;
        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(durable.to_owned()),
                filter_subject: filter_subject.to_owned(),
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: Duration::from_secs(30),
                ..Default::default()
            })
            .await?;
        client.drain().await?;
        Ok(())
    }

    /// Creates the stream the archive facts are published into, as Edge does.
    ///
    /// # Errors
    ///
    /// When the setup user cannot connect or the stream cannot be created.
    pub async fn provision_events_stream(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.admin_client().await?;
        async_nats::jetstream::new(client.clone())
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: "ratatoskr_events".to_owned(),
                subjects: vec!["evt.>".to_owned()],
                ..Default::default()
            })
            .await?;
        client.drain().await?;
        Ok(())
    }

    /// Creates the events stream with the given duplicate window and the commands stream, shaped
    /// like the two streams of the deployment, so a test can watch the pump publish a fact and an
    /// erasure request.
    ///
    /// # Errors
    ///
    /// When the setup user cannot connect or a stream cannot be created.
    pub async fn provision_streams(
        &self,
        duplicate_window: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = self.admin_client().await?;
        let jetstream = async_nats::jetstream::new(client.clone());
        jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: "ratatoskr_events".to_owned(),
                subjects: vec!["evt.>".to_owned()],
                duplicate_window,
                ..Default::default()
            })
            .await?;
        jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: "ratatoskr_commands".to_owned(),
                subjects: vec!["cmd.>".to_owned()],
                ..Default::default()
            })
            .await?;
        client.drain().await?;
        Ok(())
    }
}

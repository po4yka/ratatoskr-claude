//! Deterministic ZIP encoding and atomic private publication.

use super::*;

pub(super) fn write_zip(members: Vec<Member>) -> Result<Vec<u8>, PortableExportError> {
    let cursor = Cursor::new(Vec::new());
    let mut writer = zip::ZipWriter::new(cursor);
    let options = SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Stored)
        .last_modified_time(zip::DateTime::DEFAULT)
        .unix_permissions(0o644);
    for member in members {
        writer.start_file(member.path, options)?;
        writer.write_all(&member.bytes)?;
    }
    Ok(writer.finish()?.into_inner())
}

fn temporary_sibling(output: &Path) -> PathBuf {
    let name = output
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("portable-export.zip");
    output.with_file_name(format!(".{name}.{}.part", uuid::Uuid::now_v7()))
}

pub(super) fn publish_bytes(output: &Path, bytes: &[u8]) -> Result<(), PortableExportError> {
    let parent = output
        .parent()
        .ok_or(PortableExportError::InvalidOutputPath)?;
    std::fs::create_dir_all(parent)?;
    let temporary = temporary_sibling(output);
    let publication =
        write_private_file(&temporary, bytes).and_then(|()| std::fs::rename(&temporary, output));
    if publication.is_err() {
        remove_owned_temporary(&temporary);
    }
    publication.map_err(PortableExportError::Io)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    #[cfg(unix)]
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn remove_owned_temporary(path: &Path) {
    let _ignored = std::fs::remove_file(path);
}

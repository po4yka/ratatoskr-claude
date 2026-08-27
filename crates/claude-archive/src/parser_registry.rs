//! Exact-match selection of versioned Claude export parsers.

use std::sync::Arc;

use crate::export_projection::ParsedExport;
use crate::receipt::AcquisitionMode;

/// A schema structure detected from immutable archive evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedSchema {
    acquisition_mode: AcquisitionMode,
    identifier: String,
}

impl DetectedSchema {
    /// Creates a detected schema from a stable structure identifier.
    #[must_use]
    pub fn new(acquisition_mode: AcquisitionMode, identifier: impl Into<String>) -> Self {
        Self {
            acquisition_mode,
            identifier: identifier.into(),
        }
    }

    /// Returns the acquisition mode observed for the raw evidence.
    #[must_use]
    pub fn acquisition_mode(&self) -> AcquisitionMode {
        self.acquisition_mode
    }

    /// Returns the detected stable schema identifier.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }
}

/// A capability a parser explicitly declares it can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserCapability {
    /// Preserves the archive's observed structure only.
    ArchiveStructure,
    /// Produces project records.
    Projects,
    /// Produces project instruction records.
    ProjectInstructions,
    /// Produces Project Knowledge file references.
    ProjectKnowledgeFiles,
    /// Produces conversation records.
    Conversations,
    /// Produces message records.
    Messages,
    /// Produces typed message content parts.
    ContentParts,
    /// Produces first-class Artifact and Artifact-version records.
    Artifacts,
}

/// A versioned parser declaration without parser implementation behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserDescriptor {
    identifier: String,
    version: String,
    acquisition_modes: Vec<AcquisitionMode>,
    schema_identifiers: Vec<String>,
    capabilities: Vec<ParserCapability>,
}

/// Stable name and declared version of one parser implementation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserIdentity {
    identifier: String,
    version: String,
}

impl ParserIdentity {
    /// Creates an exact parser identity.
    #[must_use]
    pub fn new(identifier: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            identifier: identifier.into(),
            version: version.into(),
        }
    }

    /// Returns the stable parser identifier.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns the declared parser version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
}

/// Verified evidence supplied to one exact compiled parser.
#[derive(Debug, Clone, Copy)]
pub struct ParserExecutionInput<'a> {
    /// Structurally detected archive schema.
    pub detected_schema: &'a DetectedSchema,
    /// Bounded inert evidence bytes selected for parser execution.
    pub evidence: &'a [u8],
}

/// A content-free compiled parser failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParserExecutionError {
    /// The parser could not produce a validated projection.
    #[error("compiled archive parser failed")]
    Failed,
}

/// Executable behavior paired with one exact parser declaration.
pub trait ParserExecutor: core::fmt::Debug + Send + Sync {
    /// Parses verified evidence into a normalized export projection.
    ///
    /// # Errors
    ///
    /// Returns [`ParserExecutionError`] without embedding provider content.
    fn execute(
        &self,
        input: ParserExecutionInput<'_>,
    ) -> Result<ParsedExport, ParserExecutionError>;
}

/// One exact compatible compiled parser resolved for operator execution.
#[derive(Debug, Clone)]
pub struct CompiledParser {
    identity: ParserIdentity,
    executor: Arc<dyn ParserExecutor>,
}

impl CompiledParser {
    /// Returns the exact declared parser identity.
    #[must_use]
    pub fn identity(&self) -> &ParserIdentity {
        &self.identity
    }

    /// Executes this exact parser over verified evidence.
    ///
    /// # Errors
    ///
    /// Returns [`ParserExecutionError`] without embedding provider content.
    pub fn execute(
        &self,
        input: ParserExecutionInput<'_>,
    ) -> Result<ParsedExport, ParserExecutionError> {
        self.executor.execute(input)
    }
}

impl ParserDescriptor {
    /// Declares the exact boundaries of one versioned parser.
    #[must_use]
    pub fn new(
        identifier: impl Into<String>,
        version: impl Into<String>,
        acquisition_modes: Vec<AcquisitionMode>,
        schema_identifiers: Vec<String>,
        capabilities: Vec<ParserCapability>,
    ) -> Self {
        Self {
            identifier: identifier.into(),
            version: version.into(),
            acquisition_modes,
            schema_identifiers,
            capabilities,
        }
    }

    /// Returns the stable parser identifier.
    #[must_use]
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Returns the parser implementation version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Returns the declared acquisition mode support.
    #[must_use]
    pub fn acquisition_modes(&self) -> &[AcquisitionMode] {
        &self.acquisition_modes
    }

    /// Returns the exact schema identifiers this parser supports.
    #[must_use]
    pub fn schema_identifiers(&self) -> &[String] {
        &self.schema_identifiers
    }

    /// Returns the capabilities this parser can produce.
    #[must_use]
    pub fn capabilities(&self) -> &[ParserCapability] {
        &self.capabilities
    }
}

/// A parser declaration registry.
#[derive(Debug, Clone, Default)]
pub struct ParserRegistry {
    descriptors: Vec<ParserDescriptor>,
    compiled: Vec<Option<Arc<dyn ParserExecutor>>>,
}

impl ParserRegistry {
    /// Creates a registry of parser declarations.
    ///
    /// # Errors
    ///
    /// Returns [`ParserRegistryError`] when declarations overlap.
    pub fn new(descriptors: Vec<ParserDescriptor>) -> Result<Self, ParserRegistryError> {
        for (index, descriptor) in descriptors.iter().enumerate() {
            if descriptors
                .get(index.saturating_add(1)..)
                .unwrap_or_default()
                .iter()
                .any(|other| same_identity(descriptor, other))
            {
                return Err(ParserRegistryError::OverlappingDeclaration);
            }
        }

        Ok(Self {
            compiled: vec![None; descriptors.len()],
            descriptors,
        })
    }

    /// Registers a unique declaration together with compiled behavior.
    ///
    /// # Errors
    ///
    /// Returns [`ParserRegistryError::DuplicateIdentity`] when the exact name
    /// and version are already registered.
    pub fn register_compiled(
        &mut self,
        descriptor: ParserDescriptor,
        executor: Arc<dyn ParserExecutor>,
    ) -> Result<(), ParserRegistryError> {
        if self.descriptors.iter().any(|registered| {
            registered.identifier == descriptor.identifier
                && registered.version == descriptor.version
        }) {
            return Err(ParserRegistryError::DuplicateIdentity);
        }
        self.descriptors.push(descriptor);
        self.compiled.push(Some(executor));
        Ok(())
    }

    /// Lists every compatible identity in deterministic declared-version order.
    #[must_use]
    pub fn compatible_versions(
        &self,
        detected: &DetectedSchema,
        required_capabilities: &[ParserCapability],
    ) -> Vec<ParserIdentity> {
        let mut identities = self
            .descriptors
            .iter()
            .filter(|descriptor| compatible(descriptor, detected, required_capabilities))
            .map(|descriptor| ParserIdentity::new(&descriptor.identifier, &descriptor.version))
            .collect::<Vec<_>>();
        identities.sort_by(|left, right| {
            left.identifier
                .cmp(&right.identifier)
                .then_with(|| compare_versions(&left.version, &right.version))
        });
        identities
    }

    /// Resolves one exact compatible compiled identity without auto-selection.
    #[must_use]
    pub fn find_exact(
        &self,
        identity: &ParserIdentity,
        detected: &DetectedSchema,
        required_capabilities: &[ParserCapability],
    ) -> Option<CompiledParser> {
        self.descriptors
            .iter()
            .enumerate()
            .find(|(_, descriptor)| {
                descriptor.identifier == identity.identifier
                    && descriptor.version == identity.version
                    && compatible(descriptor, detected, required_capabilities)
            })
            .and_then(|(index, _)| self.compiled.get(index).and_then(Option::as_ref))
            .map(|executor| CompiledParser {
                identity: identity.clone(),
                executor: Arc::clone(executor),
            })
    }

    /// Selects one parser that exactly supports the detected schema.
    ///
    /// # Errors
    ///
    /// Returns [`ParserSelectionError`] when no parser declares exact support.
    pub fn select(
        &self,
        detected: &DetectedSchema,
        required_capabilities: &[ParserCapability],
    ) -> Result<&ParserDescriptor, ParserSelectionError> {
        let mut matches = self.descriptors.iter().filter(|descriptor| {
            descriptor
                .acquisition_modes
                .contains(&detected.acquisition_mode)
                && descriptor
                    .schema_identifiers
                    .iter()
                    .any(|identifier| identifier == &detected.identifier)
        });

        let Some(selected) = matches.next() else {
            return Err(ParserSelectionError::UnsupportedSchema);
        };

        if matches.next().is_some() {
            return Err(ParserSelectionError::AmbiguousDeclaration);
        }

        if required_capabilities
            .iter()
            .all(|capability| selected.capabilities.contains(capability))
        {
            Ok(selected)
        } else {
            Err(ParserSelectionError::UnsupportedCapabilities)
        }
    }
}

fn same_identity(left: &ParserDescriptor, right: &ParserDescriptor) -> bool {
    left.identifier == right.identifier && left.version == right.version
}

fn compatible(
    descriptor: &ParserDescriptor,
    detected: &DetectedSchema,
    required_capabilities: &[ParserCapability],
) -> bool {
    descriptor
        .acquisition_modes
        .contains(&detected.acquisition_mode)
        && descriptor.schema_identifiers.contains(&detected.identifier)
        && required_capabilities
            .iter()
            .all(|capability| descriptor.capabilities.contains(capability))
}

fn compare_versions(left: &str, right: &str) -> std::cmp::Ordering {
    let components = |version: &str| {
        version
            .split('.')
            .map(str::parse::<u64>)
            .collect::<Result<Vec<_>, _>>()
    };
    match (components(left), components(right)) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

/// Registry construction failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParserRegistryError {
    /// Two declarations claim the same acquisition-mode/schema pair.
    #[error("parser declarations overlap")]
    OverlappingDeclaration,
    /// One exact parser name and version was registered more than once.
    #[error("parser identity is already registered")]
    DuplicateIdentity,
}

/// Exact parser selection outcome when no parser may be used.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParserSelectionError {
    /// No parser declares the detected schema identifier and acquisition mode.
    #[error("no parser supports the detected schema")]
    UnsupportedSchema,
    /// Matching parsers omit one or more required capabilities.
    #[error("no parser supports the requested capabilities")]
    UnsupportedCapabilities,
    /// More than one parser declares exact support for the detected schema.
    #[error("multiple parsers support the detected schema")]
    AmbiguousDeclaration,
}

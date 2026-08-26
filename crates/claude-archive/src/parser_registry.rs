//! Exact-match selection of versioned Claude export parsers.

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
#[derive(Debug, Clone)]
pub struct ParserRegistry {
    descriptors: Vec<ParserDescriptor>,
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
                .any(|other| declarations_overlap(descriptor, other))
            {
                return Err(ParserRegistryError::OverlappingDeclaration);
            }
        }

        Ok(Self { descriptors })
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

fn declarations_overlap(left: &ParserDescriptor, right: &ParserDescriptor) -> bool {
    left.acquisition_modes
        .iter()
        .any(|mode| right.acquisition_modes.contains(mode))
        && left
            .schema_identifiers
            .iter()
            .any(|identifier| right.schema_identifiers.contains(identifier))
}

/// Registry construction failure.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ParserRegistryError {
    /// Two declarations claim the same acquisition-mode/schema pair.
    #[error("parser declarations overlap")]
    OverlappingDeclaration,
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

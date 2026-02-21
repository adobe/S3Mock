---
name: document
description: Generate or update project documentation including README, API docs, and architecture guides. Use when asked to document code, create documentation, or explain project structure.
---

# Documentation Skill for S3Mock

This skill helps generate and maintain comprehensive documentation for the S3Mock project.

## When to Use

- Creating or updating README files
- Generating API documentation
- Documenting architecture and design decisions
- Creating usage guides and examples
- Explaining S3 operations and implementations

## Instructions

When documenting S3Mock, follow these steps:

1. **Understand Context**
   - Review the specific component or feature to document
   - Check existing documentation structure in README.md
   - Identify the target audience (developers, users, contributors)

2. **Gather Information**
   - Read relevant source code in `server/src/main/`
   - Review test files for usage examples
   - Check integration tests in `integration-tests/src/test/`
   - Reference AWS S3 API documentation for accuracy

3. **Document Structure**
   - Use clear, concise language
   - Include code examples where applicable
   - Add links to related documentation
   - Follow existing documentation style in README.md
   - Include both Kotlin and Java examples when relevant

4. **Key Areas to Document**
   - **Configuration**: Environment variables and setup options
   - **API Operations**: Supported S3 operations and limitations
   - **Usage Examples**: Docker, TestContainers, JUnit, TestNG
   - **File System**: Storage structure and metadata
   - **Integration**: SDK configuration and best practices

5. **Documentation Types**

   **API Documentation**:
   - Operation name and AWS S3 API link
   - Support status (✓ or ✗)
   - Limitations or known issues
   - Example usage with AWS SDK v1 and v2

   **Configuration Documentation**:
   - Environment variable name
   - Default value
   - Description and use cases
   - Example values

   **Architecture Documentation**:
   - Component purpose and responsibilities
   - Key classes and their relationships
   - Data flow and storage patterns
   - Design decisions and trade-offs

6. **Examples and Code Snippets**
   - Provide complete, working examples
   - Include necessary imports and setup
   - Show both HTTP/HTTPS usage
   - Demonstrate error handling
   - Reference real test files when possible

7. **Quality Checklist**
   - [ ] Technical accuracy verified
   - [ ] Code examples tested
   - [ ] Links are valid
   - [ ] Consistent with existing docs
   - [ ] Clear and understandable
   - [ ] Proper markdown formatting

## Project-Specific Guidelines

### S3Mock Technical Details

- **Primary Language**: Kotlin with Java support
- **Framework**: Spring Boot 4.x
- **Testing**: JUnit 5, TestNG, Testcontainers
- **Build Tool**: Maven
- **Container**: Docker (Alpine Linux base)
- **Ports**: HTTP (9090), HTTPS (9191)

### Important Limitations to Document

- Path-style access only (no domain-style)
- Presigned URLs accepted but parameters ignored
- Self-signed SSL certificate included
- Mock implementation - not for production
- Limited KMS encryption support (validation only)

### Documentation Style

- Use GitHub-flavored Markdown
- Include badges for status and versions
- Provide table of contents for long documents
- Use code blocks with language specification
- Add shell prompts (`$`) for CLI examples
- Include "⚠️ WARNING" or "ℹ️ FYI" callouts for important notes

## Output Format

Provide documentation in Markdown format, ready to be integrated into:
- README.md sections
- Separate documentation files
- Code comments and KDoc/Javadoc
- GitHub wiki pages
- CONTRIBUTING.md guidelines

## Resources

Refer to these files for context:
- `/README.md` - Main project documentation
- `/CONTRIBUTING.md` - Contribution guidelines
- `/server/src/main/kotlin/` - Core implementation
- `/integration-tests/` - Usage examples
- `/testsupport/` - Test framework integrations

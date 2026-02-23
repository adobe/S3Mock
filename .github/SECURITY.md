# Security Policy

## Important Notice

S3Mock is a **testing tool** designed for local integration testing. It is **not intended for production use** and lacks the security features required for production environments. Do not expose S3Mock to untrusted networks or use it to store sensitive data.

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 5.x     | :white_check_mark: |
| 4.x     | :x:                |
| 3.x     | :x:                |
| < 3.0   | :x:                |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report vulnerabilities using one of the following methods:

1. **GitHub Security Advisories** (preferred): Use the [private vulnerability reporting](https://github.com/adobe/S3Mock/security/advisories/new) feature to submit a report directly through GitHub.

2. **Email**: Send a report to [Grp-opensourceoffice@adobe.com](mailto:Grp-opensourceoffice@adobe.com).

### What to Include

- Type of vulnerability
- Affected version(s)
- Steps to reproduce
- Impact assessment
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment**: Within 5 business days
- **Initial assessment**: Within 10 business days
- **Resolution**: Depending on severity and complexity

## Security Measures

S3Mock uses the following automated security tools:

- **CodeQL**: Static analysis for security vulnerabilities (via GitHub Actions)
- **SBOM**: Software Bill of Materials generation for dependency tracking
- **OpenSSF Scorecard**: Security health assessment
- **Dependabot**: Automated dependency updates for Maven, Docker, and GitHub Actions
- **Dependency Review**: Automated review of dependency changes in PRs

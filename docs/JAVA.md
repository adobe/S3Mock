# Java Guidelines — S3Mock

Canonical reference for Java idioms, naming conventions, and code quality standards used across this project.

## Style

Java code follows the **[Google Java Style Guide](https://google.github.io/styleguide/javaguide.html)** enforced by Checkstyle (`etc/checkstyle.xml`).

Key rules:
- **Indentation**: 2 spaces (no tabs)
- **Line length**: 120 characters maximum
- **Braces**: Always use braces for `if`, `for`, `while`, `do` blocks
- **Imports**: Static imports first, then third-party packages; alphabetical within groups; no wildcard imports

## Modern Java Idioms

### Local Type Inference
- Use `var` for local variables when the type is clear from context:
  ```java
  var uploadFile = new File(UPLOAD_FILE_NAME);
  var response = s3Client.getObject(...);
  ```
- Avoid `var` when the inferred type would be ambiguous or unclear

### Collections
- `list.size() == 0` / `list.size() > 0` → `list.isEmpty()` / `!list.isEmpty()`
- Use `List.of(...)`, `Map.of(...)` for immutable collections instead of `Collections.unmodifiableList(...)`
- Prefer streams over explicit loops for transformations:
  ```java
  buckets.stream().map(Bucket::name).collect(Collectors.toSet())
  ```

### Switch Expressions
- Prefer switch expressions over `if-else` chains with 3+ branches

### Text Blocks
- Use text blocks for multi-line strings

## Common Anti-Patterns

| Anti-Pattern | Refactor To |
|---|---|
| `list.size() == 0` | `list.isEmpty()` |
| `Collections.emptyList()` | `List.of()` |
| `Collections.unmodifiableList(new ArrayList<>(...))` | `List.copyOf(...)` |
| `"" + value` | `String.valueOf(value)` or `String.format(...)` |
| Empty catch blocks | At minimum, log the exception |
| Magic numbers/strings | Named constants |

## Naming Conventions

Follows [Google Java Style](https://google.github.io/styleguide/javaguide.html#s5-naming):
- **Classes/Interfaces/Enums**: `UpperCamelCase`
- **Methods/Variables**: `lowerCamelCase`
- **Constants** (`static final`): `UPPER_SNAKE_CASE`
- **Booleans**: `is-`/`has-`/`should-`/`can-` prefixes
- **Collections**: plural nouns
- **Avoid abbreviations**: `bucketMetadata` not `bktMd`
- **Abbreviations** in type names: at most 4 consecutive uppercase letters (e.g., `KmsKeyRef` not `KMSKeyRef`)

## Test Naming

- **Method names**: Use descriptive verb phrases — `shouldUploadAndDownloadObject`, `defaultBucketsGotCreated`
- **Avoid**: generic names like `testSomething` or `test1`
- **Pattern**: Arrange-Act-Assert

## Javadoc

- Use `/** */` for public APIs; `//` inline comments for rationale
- Comments explain **why**, never **what** — remove comments that restate the code
- Javadoc tag order: `@param`, `@return`, `@throws`, `@deprecated`
- Add comments for edge cases, non-obvious S3 semantics, or workarounds
- Link to AWS API docs or GitHub issues where relevant
- Single-line Javadoc is allowed: `/** Short description. */`

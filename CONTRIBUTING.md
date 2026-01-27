# Contributing to csync

Thank you for your interest in contributing to csync! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

Be respectful and inclusive. We welcome contributions from everyone and are committed to providing a welcoming and inspiring community.

## How to Contribute

### Reporting Issues

Found a bug? We'd love to hear about it! Please open a GitHub issue with:

- **Clear description** - What is the problem?
- **Reproduction steps** - How can we reproduce it?
- **Expected behavior** - What should happen?
- **Actual behavior** - What actually happens?
- **Environment** - Go version, OS, directory size, file count, etc.

### Suggesting Improvements

Have an idea for improvement? Please open an issue to discuss before implementing:

- **Describe the improvement** - What would you like to add or change?
- **Motivation** - Why would this be useful?
- **Examples** - Show how it would be used

### Pull Requests

We accept pull requests! Here's the process:

1. **Fork** the repository
2. **Create a branch** from `main` with a descriptive name: `git checkout -b feature/your-feature-name`
3. **Make changes** following the guidelines below
4. **Test thoroughly** - ensure all tests pass
5. **Commit** with clear messages
6. **Push** to your fork
7. **Open a pull request** with a clear description

## Development Setup

### Prerequisites

- Go 1.21 or later
- Git
- Basic familiarity with Go

### Getting Started

```bash
# Clone the repository
git clone https://github.com/otuschhoff/csync.git
cd csync

# Run tests
go test -v

# Run with race detector
go test -v -race

# Build
go build ./...
```

## Testing Requirements

All contributions must include:

- **Tests pass locally** - Run `go test -v` before submitting
- **Race detector passes** - Run `go test -v -race` to verify thread safety
- **No breaking changes** - Maintain backward compatibility when possible
- **Documentation** - Update relevant documentation

### Running Tests

```bash
# Run all tests
go test -v

# Run specific test
go test -v -run TestBasicSync

# Run with coverage
go test -cover

# Run with race detector
go test -v -race
```

## Code Style Guidelines

### Go Code Style

We follow standard Go conventions:

1. **Format code** with `gofmt` or your IDE
2. **Naming** - Use clear, descriptive names
3. **Comments** - Add GoDoc comments to exported types and functions
4. **Error handling** - Don't ignore errors
5. **Keep functions focused** - Single responsibility principle

### Documentation

- **Export comments** - All exported types and functions need GoDoc comments
- **Examples** - Include examples in comments where helpful
- **README updates** - Update README.md if adding features
- **Comments** - Explain the "why", not just the "what"

Example:

```go
// NewSynchronizer creates a new Synchronizer for syncing srcRoot to dstRoot.
// numWorkers specifies the number of parallel workers to use (minimum 1).
// readOnly, when true, prevents any modifications to the destination.
func NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer {
	// implementation
}
```

### Commit Messages

Write clear commit messages:

- **First line** - Brief summary (50 chars or less)
- **Blank line** - Separate subject from body
- **Body** - Explain what and why (not how)
- **Reference issues** - "Fixes #123" or "Related to #456"

Example:

```
Add work stealing for load balancing

Implement work stealing algorithm to distribute tasks among
idle workers. This improves performance when processing
unbalanced directory trees.

Fixes #42
```

## Areas for Contribution

We're particularly interested in:

- **Bug fixes** - Help us squash bugs
- **Performance improvements** - Optimize critical paths
- **Documentation** - Improve README, examples, and comments
- **Tests** - Add tests for edge cases
- **Examples** - Share how you're using csync
- **Feedback** - Report issues and suggest improvements

## Questions?

Feel free to:

- **Open an issue** - For questions about the project
- **Email** - Contact mail@oliver-tuschhoff.de
- **Discussions** - Use GitHub Discussions for general questions

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Recognition

We appreciate all contributions! Contributors will be recognized in:

- Commit history
- Pull request discussions
- Release notes (for significant contributions)

## Getting Help

- **Go Documentation** - https://golang.org/doc/
- **Go Code Review Comments** - https://golang.org/wiki/CodeReviewComments
- **GitHub Help** - https://help.github.com/

Thank you for contributing to csync! 🚀
